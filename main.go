package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/citusdata/pg_warp/consumer"
	"github.com/jackc/pgx"
	flag "github.com/ogier/pflag"
)

func truncateDestinationTables(destinationConn *pgx.Conn, tables map[string][]string) error {
	truncateTables := []string{}
	for table := range tables {
		truncateTables = append(truncateTables, table)
	}

	if len(truncateTables) > 0 {
		sql := fmt.Sprintf("TRUNCATE %s", strings.Join(truncateTables, ", "))
		_, err := destinationConn.Exec(sql)
		if err != nil {
			return fmt.Errorf("Failed to truncate on destination: %s\nSQL: %s", err, sql)
		}
	}
	return nil
}

func printProgress(message string) {
	fmt.Printf("%s: %s\n", time.Now().Format("2006-01-02 15:04:05.000 MST"), message)
}

func makeInitialBackup(sourceURL string, destinationURL string, snapshotName string, syncSchema bool, parallelDump uint32, parallelRestore uint32, tmpDir string, includedTables []string, excludedTables []string) error {
	dumpCommand := fmt.Sprintf("pg_dump --snapshot=%s -d %s", snapshotName, sourceURL)
	restoreCommand := fmt.Sprintf("pg_restore --no-acl --no-owner -d %s", destinationURL)
	if syncSchema {
		restoreCommand += " --clean"
	} else {
		restoreCommand += " --data-only"
	}

	for _, includeTable := range includedTables {
		dumpCommand += " -t" + includeTable
	}
	for _, excludeTable := range excludedTables {
		dumpCommand += " -T" + excludeTable
	}

	if parallelDump > 1 || parallelRestore > 1 || syncSchema {
		dumpDir := path.Join(tmpDir, "dump")
		err := os.MkdirAll(dumpDir, 0755)
		if err != nil {
			return fmt.Errorf("Could not create dump directory: %s\n", err)
		}

		dumpCommand += fmt.Sprintf(" -Fd -j %d -f %s", parallelDump, dumpDir)
		restoreCommand += fmt.Sprintf(" -Fd -j%d %s", parallelRestore, dumpDir)

		if parallelDump > 1 {
			printProgress(fmt.Sprintf("Running initial backup dump (%d parallel jobs)...", parallelDump))
		} else {
			printProgress("Running initial backup dump...")
		}
		cmd := exec.Command("/bin/sh", "-c", dumpCommand)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stdout
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("Error dumping data: %s\n", err)
		}

		// For schema+data dumps we need to filter out bogus entries from the ToC
		// that would require superuser access on the destination database
		if syncSchema {
			var out []byte

			out, err = exec.Command("/bin/sh", "-c", fmt.Sprintf("pg_restore -l -Fd %s", dumpDir)).Output()
			if err != nil {
				return fmt.Errorf("Error writing pg_restore ToC file: %s\n", err)
			}

			tocIn := strings.Split(string(out), "\n")
			tocOut := []string{}
			for _, tocLine := range tocIn {
				if strings.HasPrefix(tocLine, ";") {
					continue
				}
				parts := strings.SplitN(tocLine, " ", 5)
				if len(parts) < 5 {
					continue
				}
				switch parts[3] {
				case "ACL", "COMMENT", "DATABASE", "EXTENSION":
					continue
				case "SCHEMA":
					if strings.HasPrefix(parts[4], "- public") {
						continue
					}
				}
				tocOut = append(tocOut, tocLine)
			}

			tocPath := path.Join(tmpDir, "pg_restore_toc")
			err = ioutil.WriteFile(tocPath, []byte(strings.Join(tocOut, "\n")), 0744)
			if err != nil {
				return fmt.Errorf("Error writing ToC for pg_restore: %s\n", err)
			}

			defer os.Remove(tocPath)
			restoreCommand += " -L" + tocPath
		}

		if parallelRestore > 1 {
			printProgress(fmt.Sprintf("Running initial backup restore (%d parallel jobs)...", parallelRestore))
		} else {
			printProgress("Running initial backup restore...")
		}
		cmd = exec.Command("/bin/sh", "-c", restoreCommand)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stdout
		err = cmd.Run()
		if err != nil {
			return fmt.Errorf("Error restoring data: %s\n", err)
		}

		err = os.RemoveAll(dumpDir)
		if err != nil {
			return fmt.Errorf("Could not remove dump directory: %s\n", err)
		}
	} else {
		printProgress("Running initial backup...")

		dumpCommand += " -Fc"
		cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("%s | %s", dumpCommand, restoreCommand))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stdout
		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("Error copying data: %s\n", err)
		}
	}
	return nil
}

func handleDecodingMessage(message string, destinationConn *pgx.Conn, tables map[string][]string, distributedTables map[string]string, stats *logStats) error {
	sql, sqlType := consumer.Decode(message, tables, distributedTables)
	if sql != "" {
		_, err := destinationConn.Exec(sql)
		if err != nil {
			return fmt.Errorf("Could not apply stream to destination: %s\n  SQL: %s", err, sql)
		}
	}
	switch sqlType {
	case "COMMIT":
		stats.tx++
	case "INSERT":
		stats.inserts++
	case "UPDATE":
		stats.updates++
	case "DELETE":
		stats.deletes++
	}
	return nil
}

type logStats struct {
	start   time.Time
	tx      uint64
	inserts uint64
	updates uint64
	deletes uint64
}

func (s logStats) secondsElapsed() float64 {
	return float64(time.Since(s.start)) / float64(time.Second)
}

func (s logStats) txPerSecond() float64 {
	return float64(s.tx) / s.secondsElapsed()
}

func (s logStats) insertsPerSecond() float64 {
	return float64(s.inserts) / s.secondsElapsed()
}

func (s logStats) updatesPerSecond() float64 {
	return float64(s.updates) / s.secondsElapsed()
}

func (s logStats) deletesPerSecond() float64 {
	return float64(s.deletes) / s.secondsElapsed()
}

func retrieveLagAndOutputStats(sourceConn *pgx.Conn, stats logStats, maxWal uint64) {
	var replicationLag string
	var err error

	err = sourceConn.QueryRow("SELECT COALESCE(pg_size_pretty(pg_xlog_location_diff(pg_current_xlog_flush_location(), replay_location)), 'n/a') FROM pg_stat_replication WHERE application_name = 'pg_warp'").Scan(&replicationLag)
	if err != nil {
		if strings.HasPrefix(err.Error(), "ERROR: function pg_current_xlog_flush_location() does not exist") { // pre 9.6
			replicationLag = "unknown"
		} else {
			fmt.Printf("ERROR: Failed to retrieve replication lag: %s\n", err)
			os.Exit(1)
		}
	}

	// TODO: Add timestamp of last transaction replayed (after that COMMIT happened)
	printProgress(fmt.Sprintf("Stats: %0.2f TX/s, %0.2f inserts/s, %0.2f updates/s, %0.2f deletes/s, last LSN received: %s, replication lag: %s",
		stats.txPerSecond(), stats.insertsPerSecond(), stats.updatesPerSecond(), stats.deletesPerSecond(), pgx.FormatLSN(maxWal), replicationLag))
}

func startReplication(slotName string, replicationConn *pgx.ReplicationConn, sourceConn *pgx.Conn, destinationConn *pgx.Conn, tables map[string][]string, distributedTables map[string]string, originName string, lastCommittedSourceLsn string) (chan bool, error) {
	var err error

	stop := make(chan bool)

	startLsn := uint64(0)
	if lastCommittedSourceLsn != "" {
		startLsn, err = pgx.ParseLSN(lastCommittedSourceLsn)
		if err != nil {
			return stop, fmt.Errorf("Failed to parse last committed source LSN: %s", err)
		}
	}

	err = replicationConn.StartReplication(slotName, startLsn, -1)
	if err != nil {
		return stop, fmt.Errorf("Failed to start replication: %v", err)
	}

	printProgress("Started replication")

	_, err = destinationConn.Exec(fmt.Sprintf("SELECT pg_replication_origin_session_setup('%s')", originName))
	if err != nil {
		return stop, fmt.Errorf("Failed to setup replication origin session on destination: %v", err)
	}

	standbyStatusInterval := time.Duration(1 * time.Second)
	logInterval := time.Duration(5 * time.Second)

	go func() {
		var maxWal uint64

		stats := logStats{start: time.Now()}

		standbyStatusTick := time.Tick(standbyStatusInterval)
		logTick := time.Tick(logInterval)

		for {
			var message *pgx.ReplicationMessage
			var status *pgx.StandbyStatus
			var sendStandbyStatus bool

			select {
			case <-standbyStatusTick:
				sendStandbyStatus = true
			case <-logTick:
				// TODO: This will be a problem if we ever need more than 5s to retrieve
				// the lag state from the source connection (since we're missing a mutex)
				go retrieveLagAndOutputStats(sourceConn, stats, maxWal)
				stats = logStats{start: time.Now()}
			case <-stop:
				replicationConn.Close()
				return
			default:
				message, err = replicationConn.WaitForReplicationMessage(standbyStatusInterval)
				if err != nil {
					if err == io.EOF {
						fmt.Printf("ERROR: End of stream (EOF), stopping replication\n")
						os.Exit(1)
					}
					if err != pgx.ErrNotificationTimeout {
						fmt.Printf("ERROR: Replication failed: %v %s\n", err, reflect.TypeOf(err))
						os.Exit(1)
					}
				}
			}

			if message != nil {
				if message.WalMessage != nil {
					walString := string(message.WalMessage.WalData)

					if strings.HasPrefix(walString, "COMMIT") {
						// FIXME: Replace now() with the timestamp from the COMMIT message (when running with include-timestamps)
						_, err = destinationConn.Exec(fmt.Sprintf("SELECT pg_replication_origin_xact_setup('%s', now()); SELECT txid_current();", pgx.FormatLSN(message.WalMessage.WalStart)))
						if err != nil {
							fmt.Printf("ERROR: Could not setup replication origin for transaction on destination: %s\n", err)
							os.Exit(1)
						}
					}

					err = handleDecodingMessage(walString, destinationConn, tables, distributedTables, &stats)
					if err != nil {
						fmt.Printf("ERROR: Failed to handle WAL message: %s\n", err)
						os.Exit(1)
					}

					if message.WalMessage.WalStart > maxWal {
						maxWal = message.WalMessage.WalStart
					}
				}
				if message.ServerHeartbeat != nil && message.ServerHeartbeat.ReplyRequested == '1' {
					sendStandbyStatus = true
				}
			}

			if sendStandbyStatus {
				var lsnStr string
				var lsn uint64

				err = destinationConn.QueryRow("SELECT COALESCE(pg_replication_origin_session_progress(true), '0/0')").Scan(&lsnStr)
				if err != nil {
					fmt.Printf("ERROR: Failed to retrieve replication origin progress from destination session: %s\n", err)
					os.Exit(1)
				}

				lsn, err = pgx.ParseLSN(lsnStr)
				if err != nil {
					fmt.Printf("ERROR: Failed to parse destination session progress LSN: %s\n", err)
					os.Exit(1)
				}

				status, err = pgx.NewStandbyStatus(lsn, lsn, maxWal)
				if err != nil {
					fmt.Printf("ERROR: Failed to create standby status %v\n", err)
					os.Exit(1)
				}
				replicationConn.SendStandbyStatus(status)
			}
		}
	}()

	return stop, nil
}

type flagMultiString []string

func (fms *flagMultiString) String() string {
	return ""
}

func (fms *flagMultiString) Set(str string) error {
	*fms = append(*fms, str)
	return nil
}

// getPrimaryKeys - Gets primary keys from the source so we can correctly apply UPDATEs
func getPrimaryKeys(sourceConn *pgx.Conn, includedTables []string, excludedTables []string) (map[string][]string, error) {
	rows, err := sourceConn.Query("SELECT nspname, relname, array_agg(attname::text) FILTER (WHERE attname IS NOT NULL)" +
		" FROM pg_class cl JOIN pg_namespace n ON (cl.relnamespace = n.oid)" +
		" LEFT JOIN (SELECT conrelid, unnest(conkey) AS attnum FROM pg_constraint WHERE contype = 'p') co ON (cl.oid = co.conrelid)" +
		" LEFT JOIN pg_attribute a ON (a.attnum = co.attnum AND a.attrelid = co.conrelid)" +
		" WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND relpersistence = 'p' AND relkind = 'r'" +
		" GROUP BY 1, 2")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := map[string][]string{}
	for rows.Next() {
		var nspname string
		var relname string
		var cols []string
		err = rows.Scan(&nspname, &relname, &cols)
		if err != nil {
			return nil, err
		}
		skip := false
		if len(includedTables) > 0 {
			skip = true
			for _, includedTable := range includedTables {
				if includedTable == nspname+"."+relname || includedTable == relname {
					skip = false
				}
			}
		}
		if len(excludedTables) > 0 {
			for _, excludedTable := range excludedTables {
				if excludedTable == nspname+"."+relname || excludedTable == relname {
					skip = true
				}
			}
		}
		if !skip {
			tables[nspname+"."+relname] = cols
		}
	}
	return tables, nil
}

func getDistributedTables(destinationConn *pgx.Conn, includedTables []string, excludedTables []string) (map[string]string, error) {
	rows, err := destinationConn.Query("SELECT nspname, relname, part_key" +
		" FROM (SELECT nspname, relname FROM pg_dist_partition" +
		" JOIN pg_class ON (pg_class.oid = pg_dist_partition.logicalrelid)" +
		" JOIN pg_namespace ON (pg_namespace.oid = pg_class.relnamespace)) AS rels," +
		" master_get_table_metadata(nspname || '.' || relname)" +
		" WHERE part_key IS NOT NULL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	partKeyByTable := map[string]string{}
	for rows.Next() {
		var nspname string
		var relname string
		var partKey string
		err = rows.Scan(&nspname, &relname, &partKey)
		if err != nil {
			return nil, err
		}
		skip := false
		if len(includedTables) > 0 {
			skip = true
			for _, includedTable := range includedTables {
				if includedTable == nspname+"."+relname || includedTable == relname {
					skip = false
				}
			}
		}
		if len(excludedTables) > 0 {
			for _, excludedTable := range excludedTables {
				if excludedTable == nspname+"."+relname || excludedTable == relname {
					skip = true
				}
			}
		}
		if !skip {
			partKeyByTable[nspname+"."+relname] = partKey
		}
	}
	return partKeyByTable, nil
}

func openSourceConn(sourceURL string) (*pgx.Conn, *pgx.ReplicationConn, error) {
	sourceConnConfig, err := pgx.ParseURI(sourceURL)
	if err != nil {
		return nil, nil, err
	}

	// pgx misinterprets sslmode=require as sslmode=verify-full (and fails typically because it misses the CA)
	sourceConnConfig.TLSConfig.InsecureSkipVerify = true
	sourceConnConfig.RuntimeParams["application_name"] = "pg_warp"

	sourceConn, err := pgx.Connect(sourceConnConfig)
	if err != nil {
		return nil, nil, err
	}

	replicationConn, err := pgx.ReplicationConnect(sourceConnConfig)
	if err != nil {
		sourceConn.Close()
		return nil, nil, fmt.Errorf("Could not open replication connection: %v", err)
	}

	return sourceConn, replicationConn, nil
}

func openDestinationConn(destinationURL string) (*pgx.Conn, error) {
	destinationConnConfig, err := pgx.ParseURI(destinationURL)
	if err != nil {
		return nil, err
	}

	// pgx misinterprets sslmode=require as sslmode=verify-full (and fails typically because it misses the CA)
	destinationConnConfig.TLSConfig.InsecureSkipVerify = true
	destinationConnConfig.RuntimeParams["application_name"] = "pg_warp"

	destinationConn, err := pgx.Connect(destinationConnConfig)
	if err != nil {
		return nil, err
	}

	// Disable synchronous commit to optimize performance on EBS-backed databases
	_, err = destinationConn.Exec("SET synchronous_commit = off")
	if err != nil {
		destinationConn.Close()
		return nil, fmt.Errorf("Could not disable synchronous_commit: %s", err)
	}

	// Disable Citus deadlock prevention to allow multi-shard modifications
	_, err = destinationConn.Exec("SET citus.enable_deadlock_prevention = off")
	if err != nil {
		destinationConn.Close()
		return nil, fmt.Errorf("Could not disable citus.enable_deadlock_prevention: %s", err)
	}

	// Support public.pg_replication_origin security definer functions on destination
	_, err = destinationConn.Exec("SET search_path = public, pg_catalog")
	if err != nil {
		destinationConn.Close()
		return nil, fmt.Errorf("Could not set search path on destination: %s", err)
	}

	return destinationConn, nil
}

func main() {
	var slotName string
	var originName string
	var sourceURL string
	var destinationURL string
	var resync bool
	var clean bool
	var skipInitialSync bool
	var syncSchema bool
	var parallelDump uint32
	var parallelRestore uint32
	var tmpDir string
	var includedTables flagMultiString
	var excludedTables flagMultiString

	flag.BoolVar(&skipInitialSync, "skip-initial-sync", false, "Skips initial sync (initial backup) and directly starts replication")
	flag.BoolVar(&syncSchema, "sync-schema", false, "Include schema in the initial backup process (this will remove any existing objects on the target)")
	flag.StringVar(&slotName, "slot", "pg_warp", "Name of the logical replication slot on the source database")
	flag.StringVar(&originName, "origin", "pg_warp", "Name of the replication origin on the destination database")
	flag.StringVarP(&sourceURL, "source", "s", "", "postgres:// connection string for the source database (user needs replication privileges)")
	flag.StringVarP(&destinationURL, "destination", "d", "", "postgres:// connection string for the destination database")
	flag.Uint32Var(&parallelDump, "parallel-dump", 1, "Number of parallel operations whilst dumping the initial sync data (default 1 = not parallel)")
	flag.Uint32Var(&parallelRestore, "parallel-restore", 1, "Number of parallel operations whilst restoring the initial sync data (default 1 = not parallel)")
	flag.StringVar(&tmpDir, "tmp-dir", "", "Directory where we store temporary files as part of parallel operations (this needs to be fast and have space for your full data set)")
	flag.BoolVar(&resync, "resync", false, "Resynchronize replication with a new initial backup")
	flag.BoolVar(&clean, "clean", false, "Cleans replication slot on source, and replication origin on destination (run this after you don't need replication anymore)")
	flag.VarP(&includedTables, "table", "t", "dump, restore and replicate the named table(s) only")
	flag.VarP(&excludedTables, "exclude-table", "T", "do NOT dump, restore and replicate the named table(s)")
	flag.Parse()

	if sourceURL == "" || destinationURL == "" {
		fmt.Printf("ERROR: You need to specify both a source (-s) and destination (-d) database\n")
		return
	}

	if (parallelDump > 1 || parallelRestore > 1 || syncSchema) && tmpDir == "" {
		fmt.Printf("ERROR: You need to specify --tmp-dir when using parallel options or --sync-schema\nHINT: Pick a fast disk that has enough space for your full data set\n")
		return
	}

	sourceConn, replicationConn, err := openSourceConn(sourceURL)
	if err != nil {
		fmt.Printf("ERROR: Unable to establish source connection: %v\n", err)
		return
	}
	defer sourceConn.Close()
	defer replicationConn.Close()

	destinationConn, err := openDestinationConn(destinationURL)
	if err != nil {
		fmt.Printf("ERROR: Unable to establish destination SQL connection: %v\n", err)
		return
	}
	defer destinationConn.Close()

	tables, err := getPrimaryKeys(sourceConn, includedTables, excludedTables)
	if err != nil {
		fmt.Printf("ERROR: Could not determine primary keys on source: %v\n", err)
		return
	}

	distributedTables, err := getDistributedTables(destinationConn, includedTables, excludedTables)
	if err != nil {
		fmt.Printf("ERROR: Could not determine distributed tables on destination: %v\n", err)
		return
	}

	var lastCommittedSourceLsn string

	if resync || clean {
		_, err = sourceConn.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
		if err != nil && !strings.HasPrefix(err.Error(), fmt.Sprintf("ERROR: replication slot \"%s\" does not exist", slotName)) {
			fmt.Printf("ERROR: Could not drop replication slot: %s\n", err)
			return
		}

		_, err = destinationConn.Exec(fmt.Sprintf("SELECT pg_replication_origin_drop('%s')", originName))
		if err != nil && !strings.HasPrefix(err.Error(), fmt.Sprintf("ERROR: cache lookup failed for replication origin '%s'", originName)) {
			fmt.Printf("ERROR: Could not drop replication origin: %s\n", err)
			return
		}
	} else {
		err = destinationConn.QueryRow(fmt.Sprintf("SELECT COALESCE(pg_replication_origin_progress('%s', true), '0/0')", originName)).Scan(&lastCommittedSourceLsn)
		if err != nil && !strings.HasPrefix(err.Error(), "ERROR: cache lookup failed for replication origin") {
			fmt.Printf("ERROR: Failed to retrieve replication origin progress from destination: %s\n", err)
			os.Exit(1)
		}
	}

	if clean {
		fmt.Printf("Replication slot and origin removed successfully\n")
		return
	}

	if lastCommittedSourceLsn == "" {
		printProgress("Creating replication slot on source...")
		// TODO: Create replication slot with "skip-empty-xacts", "include-timestamps" option
		var snapshotName string
		snapshotName, err = replicationConn.CreateReplicationSlotAndGetSnapshotName(slotName, "test_decoding")
		if err != nil {
			fmt.Printf("ERROR: Could not create replication slot: %s\n", err)
			return
		}

		if !skipInitialSync {
			if !syncSchema {
				printProgress("Truncating destination tables...")
				err = truncateDestinationTables(destinationConn, tables)
				if err != nil {
					fmt.Printf("ERROR: Destination truncate failed: %s\n", err)
					return
				}
			}

			err = makeInitialBackup(sourceURL, destinationURL, snapshotName, syncSchema, parallelDump, parallelRestore, tmpDir, []string(includedTables), []string(excludedTables))
			if err != nil {
				fmt.Printf("ERROR: Initial backup failed: %s\n", err)
				err = replicationConn.DropReplicationSlot(slotName)
				if err != nil {
					fmt.Printf("ERROR: Could not drop replication slot: %s\n", err)
				}
				return
			}
		}

		_, err = destinationConn.Exec(fmt.Sprintf("SELECT pg_replication_origin_create('%s')", originName))
		if err != nil {
			fmt.Printf("%s\n", err)
			return
		}
	} else {
		printProgress(fmt.Sprintf("Resuming previous operation for slot %s, last committed source LSN: %s", slotName, lastCommittedSourceLsn))
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	stop, err := startReplication(slotName, replicationConn, sourceConn, destinationConn, tables, distributedTables, originName, lastCommittedSourceLsn)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	s := <-sigs
	fmt.Printf("Received %s, exiting...\n", s)

	stop <- true

	fmt.Printf("WARNING: Replication slot and origin still exist, re-run with \"--clean\" to remove (otherwise your source system can experience problems!)\n")
}
