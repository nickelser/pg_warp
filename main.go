package main

import (
	"fmt"
	"io"
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
	for table := range tables {
		_, err := destinationConn.Exec(fmt.Sprintf("TRUNCATE %s", table))
		if err != nil {
			return fmt.Errorf("Failed to truncate %s on destination: %s", table, err)
		}
	}
	return nil
}

func makeBaseBackup(sourceURL string, destinationURL string, snapshotName string, parallelDump uint32, parallelRestore uint32, tmpDir string, includedTables []string, excludedTables []string) error {
	dumpCommand := fmt.Sprintf("pg_dump --snapshot=%s -d %s", snapshotName, sourceURL)
	restoreCommand := fmt.Sprintf("pg_restore --data-only --no-acl --no-owner -d %s", destinationURL)

	for _, includeTable := range includedTables {
		dumpCommand += " -t" + includeTable
	}
	for _, excludeTable := range excludedTables {
		dumpCommand += " -T" + excludeTable
	}

	if parallelDump > 1 || parallelRestore > 1 {
		dumpDir := path.Join(tmpDir, "dump")
		err := os.MkdirAll(dumpDir, 0755)
		if err != nil {
			return fmt.Errorf("Could not create dump directory: %s\n", err)
		}

		dumpCommand += fmt.Sprintf("-Fd -j %d -f %s", parallelDump, dumpDir)
		restoreCommand += fmt.Sprintf("-Fd -j%d %s", parallelRestore, dumpDir)

		err = os.RemoveAll(dumpDir)
		if err != nil {
			return fmt.Errorf("Could not remove dump directory: %s\n", err)
		}
	} else {
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

func handleDecodingMessage(message string, destinationConn *pgx.Conn, tables map[string][]string, stats *logStats) error {
	sql, sqlType := consumer.Decode(message, tables)
	if sql != "" {
		_, err := destinationConn.Exec(sql)
		if err != nil {
			return fmt.Errorf("Could not apply stream to destination: %s\n  SQL: %q", err, sql)
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
		fmt.Printf("Failed to retrieve replication lag: %s\n", err)
		os.Exit(1)
	}

	// TODO: timestamp of last transaction replayed (after that COMMIT happened)
	fmt.Printf("Stats: %0.2f TX/s, %0.2f inserts/s, %0.2f updates/s, %0.2f deletes/s, last LSN received: %s, replication lag: %s\n",
		stats.txPerSecond(), stats.insertsPerSecond(), stats.updatesPerSecond(), stats.deletesPerSecond(), pgx.FormatLSN(maxWal), replicationLag)
}

func startReplication(slotName string, replicationConn *pgx.ReplicationConn, sourceConn *pgx.Conn, destinationConn *pgx.Conn, tables map[string][]string, originName string, lastCommittedSourceLsn string) (chan bool, error) {
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

	fmt.Printf("Started replication\n")

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
						fmt.Printf("End of stream (EOF), stopping replication\n")
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
							fmt.Printf("Could not setup replication origin for transaction on destination: %s", err)
							os.Exit(1)
						}
					}

					err = handleDecodingMessage(walString, destinationConn, tables, &stats)
					if err != nil {
						fmt.Printf("Failed to handle WAL message: %s\n", err)
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
					fmt.Printf("Failed to retrieve replication origin progress from destination session: %s\n", err)
					os.Exit(1)
				}

				lsn, err = pgx.ParseLSN(lsnStr)
				if err != nil {
					fmt.Printf("Failed to parse destination session progress LSN: %s\n", err)
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

func main() {
	var slotName string
	var originName string
	var sourceURL string
	var destinationURL string
	var resync bool
	var clean bool
	var skipInitialSync bool
	var parallelDump uint32
	var parallelRestore uint32
	var tmpDir string
	var includedTables flagMultiString
	var excludedTables flagMultiString

	flag.BoolVar(&skipInitialSync, "skip-initial-sync", false, "Skips initial sync (base backup) and directly starts replication")
	flag.StringVar(&slotName, "slot", "pg_warp", "Name of the logical replication slot on the source database")
	flag.StringVar(&originName, "origin", "pg_warp", "Name of the replication origin on the destination database")
	flag.StringVarP(&sourceURL, "source", "s", "", "postgres:// connection string for the source database (user needs replication privileges)")
	flag.StringVarP(&destinationURL, "destination", "d", "", "postgres:// connection string for the destination database")
	flag.Uint32Var(&parallelDump, "parallel-dump", 1, "Number of parallel operations whilst dumping the initial sync data (default 1 = not parallel)")
	flag.Uint32Var(&parallelRestore, "parallel-restore", 1, "Number of parallel operations whilst restoring the initial sync data (default 1 = not parallel)")
	flag.StringVar(&tmpDir, "tmp-dir", "", "Directory where we store temporary files (e.g. for parallel operations), this needs to be fast and have space for your full data set")
	flag.BoolVar(&resync, "resync", false, "Resynchronize replication with a new base backup")
	flag.BoolVar(&clean, "clean", false, "Cleans replication slot on source, and replication origin on destination (run this after you don't need replication anymore)")
	flag.VarP(&includedTables, "table", "t", "dump, restore and replicate the named table(s) only")
	flag.VarP(&excludedTables, "exclude-table", "T", "do NOT dump, restore and replicate the named table(s)")
	flag.Parse()

	if sourceURL == "" || destinationURL == "" {
		fmt.Printf("ERROR: You need to specify both a source (-s) and destination (-d) database\n")
		return
	}

	if (parallelDump > 1 || parallelRestore > 1) && tmpDir == "" {
		fmt.Printf("ERROR: You need to specify --tmp-dir when using parallel dump or restore options\nHINT: Pick a fast disk that has enough space for your full data set\n")
		return
	}

	sourceConnConfig, err := pgx.ParseURI(sourceURL)
	if err != nil {
		fmt.Printf("Could not parse source connection URI: %v\n", err)
		return
	}
	sourceConnConfig.RuntimeParams["application_name"] = "pg_warp"

	sourceConn, err := pgx.Connect(sourceConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish source SQL connection: %v\n", err)
		return
	}
	defer sourceConn.Close()

	// Get primary keys from the source so we can correctly apply UPDATEs
	rows, err := sourceConn.Query("SELECT nspname || '.' || relname, array_agg(attname::text) FILTER (WHERE attname IS NOT NULL)" +
		" FROM pg_class cl JOIN pg_namespace n ON (cl.relnamespace = n.oid)" +
		" LEFT JOIN (SELECT conrelid, unnest(conkey) AS attnum FROM pg_constraint WHERE contype = 'p') co ON (cl.oid = co.conrelid)" +
		" LEFT JOIN pg_attribute a ON (a.attnum = co.attnum AND a.attrelid = co.conrelid)" +
		" WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') AND relpersistence = 'p' AND relkind = 'r'" +
		" GROUP BY 1")
	if err != nil {
		return
	}
	defer rows.Close()

	tables := map[string][]string{}
	for rows.Next() {
		var table string
		var cols []string
		err = rows.Scan(&table, &cols)
		if err != nil {
			fmt.Printf("ERROR: %s\n", err)
			return
		}
		skip := false
		if len(includedTables) > 0 {
			skip = true
			for _, includedTable := range includedTables {
				if includedTable == table {
					skip = false
				}
			}
		}
		if len(excludedTables) > 0 {
			for _, excludedTable := range excludedTables {
				if excludedTable == table {
					skip = true
				}
			}
		}
		if !skip {
			tables[table] = cols
		}
	}

	destinationConnConfig, err := pgx.ParseURI(destinationURL)
	if err != nil {
		fmt.Printf("Could not parse destination connection URI: %v\n", err)
		return
	}
	// pgx misinterprets sslmode=require as sslmode=verify-full (and fails typically because it misses the CA)
	destinationConnConfig.TLSConfig.InsecureSkipVerify = true
	destinationConnConfig.RuntimeParams["application_name"] = "pg_warp"
	destinationConn, err := pgx.Connect(destinationConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish destination SQL connection: %v\n", err)
		return
	}
	defer destinationConn.Close()

	// Disable synchronous commit to optimize performance on EBS-backed databases
	_, err = destinationConn.Exec("SET synchronous_commit = off")
	if err != nil {
		fmt.Printf("Could not disable synchronous_commit: %s\n", err)
		return
	}

	// Disable Citus deadlock prevention to allow multi-shard modifications
	_, err = destinationConn.Exec("SET citus.enable_deadlock_prevention = off")
	if err != nil {
		fmt.Printf("Could not disable citus.enable_deadlock_prevention: %s\n", err)
		return
	}

	replicationConn, err := pgx.ReplicationConnect(sourceConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish source replication connection: %v\n", err)
		return
	}
	defer replicationConn.Close()

	// Support public.pg_replication_origin security definer functions on destination
	_, err = destinationConn.Exec("SET search_path = public, pg_catalog")
	if err != nil {
		fmt.Printf("Could not set search path on destination: %s\n", err)
		return
	}

	var lastCommittedSourceLsn string

	if resync || clean {
		_, err = sourceConn.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
		if err != nil && !strings.HasPrefix(err.Error(), fmt.Sprintf("ERROR: replication slot \"%s\" does not exist", slotName)) {
			fmt.Printf("Error dropping replication slot: %s\n", err)
			return
		}

		_, err = destinationConn.Exec(fmt.Sprintf("SELECT pg_replication_origin_drop('%s')", originName))
		if err != nil && !strings.HasPrefix(err.Error(), fmt.Sprintf("ERROR: cache lookup failed for replication origin '%s'", originName)) {
			fmt.Printf("Error dropping replication origin: %s\n", err)
			return
		}
	} else {
		err = destinationConn.QueryRow(fmt.Sprintf("SELECT COALESCE(pg_replication_origin_progress('%s', true), '0/0')", originName)).Scan(&lastCommittedSourceLsn)
		if err != nil && !strings.HasPrefix(err.Error(), "ERROR: cache lookup failed for replication origin") {
			fmt.Printf("Failed to retrieve replication origin progress from destination: %s\n", err)
			os.Exit(1)
		}
	}

	if clean {
		fmt.Printf("Replication slot and origin removed successfully\n")
		return
	}

	if lastCommittedSourceLsn == "" {
		fmt.Printf("Creating replication slot on source...\n")
		// Create replication slot with "skip-empty-xacts", "include-timestamps" option
		var snapshotName string
		snapshotName, err = replicationConn.CreateReplicationSlotAndGetSnapshotName(slotName, "test_decoding")
		if err != nil {
			fmt.Printf("replication slot create failed: %v\n", err)
			return
		}

		if !skipInitialSync {
			fmt.Printf("Truncating destination tables...\n")
			err = truncateDestinationTables(destinationConn, tables)
			if err != nil {
				fmt.Printf("Destination truncate failed: %s\n", err)
				return
			}

			fmt.Printf("Making base backup...\n")
			err = makeBaseBackup(sourceURL, destinationURL, snapshotName, parallelDump, parallelRestore, tmpDir, []string(includedTables), []string(excludedTables))
			if err != nil {
				fmt.Printf("Base backup failed: %s\n", err)
				err = replicationConn.DropReplicationSlot(slotName)
				if err != nil {
					fmt.Printf("Error dropping replication slot: %s\n", err)
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
		fmt.Printf("Resuming previous operation for slot %s, last committed source LSN: %s\n", slotName, lastCommittedSourceLsn)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	stop, err := startReplication(slotName, replicationConn, sourceConn, destinationConn, tables, originName, lastCommittedSourceLsn)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	s := <-sigs
	fmt.Printf("Received %s, exiting...\n", s)

	stop <- true

	fmt.Printf("WARNING: Replication slot and origin still exist, re-run with \"--clean\" to remove (otherwise your source system can experience problems!)\n")
}
