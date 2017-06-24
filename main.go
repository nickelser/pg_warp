package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/jackc/pgx"
	"github.com/citusdata/pg_warp/consumer"
	flag "github.com/ogier/pflag"
)

func truncateTargetTables(targetConn *pgx.Conn, tables map[string][]string) error {
	for table := range tables {
		_, err := targetConn.Exec(fmt.Sprintf("TRUNCATE %s", table))
		if err != nil {
			return fmt.Errorf("Failed to truncate %s on target: %s", table, err)
		}
	}
	return nil
}

func makeBaseBackup(sourceURL string, targetURL string, snapshotName string) error {
	// TODO: Write this to a directory (-Fd) and then dump and load in parallel using -p
	cmd := exec.Command("/bin/sh", "-c", fmt.Sprintf("pg_dump -Fc --snapshot=%s %s | pg_restore --data-only --no-acl --no-owner -d %s", snapshotName, sourceURL, targetURL))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Error copying data: %s\n", err)
	}
	return nil
}

func handleDecodingMessage(message string, targetConn *pgx.Conn, tables map[string][]string) error {
	//fmt.Printf("WAL Message: %s\n", message)
	sql := consumer.Decode(message, tables)
	if sql != "" {
		_, err := targetConn.Exec(sql)
		if err != nil {
			return fmt.Errorf("Could not apply stream to target: %s\n  SQL: %q", err, sql)
		}
	}
	return nil
}

func startReplication(slotName string, replicationConn *pgx.ReplicationConn, targetConn *pgx.Conn, tables map[string][]string) (chan bool, error) {
	stop := make(chan bool)

	err := replicationConn.StartReplication(slotName, 0, -1)
	if err != nil {
		return stop, fmt.Errorf("Failed to start replication: %v", err)
	}

	fmt.Printf("Started replication\n")

	// TODO: Actually read wal_sender_timeout
	walSenderTimeoutSecs := 30 * time.Second

	statusInterval := time.Duration(walSenderTimeoutSecs / 4)

	//fmt.Printf("%+v\n", tables)

	go func() {
		var maxWal uint64
		var message *pgx.ReplicationMessage
		var status *pgx.StandbyStatus

		tick := time.Tick(statusInterval)

		for {
			select {
			case <-tick:
				status, err = pgx.NewStandbyStatus(maxWal)
				if err != nil {
					fmt.Printf("ERROR: Failed to create standby status %v\n", err)
					syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
					break
				}
				replicationConn.SendStandbyStatus(status)
			case <-stop:
				replicationConn.Close()
				return
			default:
				message, err = replicationConn.WaitForReplicationMessage(statusInterval)
				if err != nil {
					if err == io.EOF {
						fmt.Printf("End of stream (EOF), stopping replication\n")
						syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
						break
					}
					if err != pgx.ErrNotificationTimeout {
						fmt.Printf("ERROR: Replication failed: %v %s\n", err, reflect.TypeOf(err))
						syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
						break
					}
				}
			}

			if message != nil {
				if message.WalMessage != nil {
					walString := string(message.WalMessage.WalData)
					err = handleDecodingMessage(walString, targetConn, tables)
					if err != nil {
						fmt.Printf("Failed to handle WAL message: %s\n", err)
					}

					if message.WalMessage.WalStart > maxWal {
						maxWal = message.WalMessage.WalStart
					}
				}
				if message.ServerHeartbeat != nil && message.ServerHeartbeat.ReplyRequested == '1' {
					status, err = pgx.NewStandbyStatus(maxWal)
					if err != nil {
						fmt.Printf("ERROR: Failed to create standby status %v\n", err)
						syscall.Kill(syscall.Getpid(), syscall.SIGTERM)
						break
					}
					replicationConn.SendStandbyStatus(status)
				}
			}
		}
	}()

	return stop, nil
}

func main() {
	var slotName string
	var sourceURL string
	var targetURL string

	flag.StringVar(&slotName, "slot", "pg_warp", "name of the logical replication slot on the source database")
	flag.StringVarP(&sourceURL, "source", "s", "", "postgres:// connection string for the source database (user needs replication privileges)")
	flag.StringVarP(&targetURL, "target", "t", "", "postgres:// connection string for the target database")
	flag.Parse()

	sourceConnConfig, err := pgx.ParseURI(sourceURL)
	if err != nil {
		fmt.Printf("Could not parse source connection URI: %v\n", err)
		return
	}

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
		tables[table] = cols
	}

	targetConnConfig, err := pgx.ParseURI(targetURL)
	if err != nil {
		fmt.Printf("Could not parse target connection URI: %v\n", err)
		return
	}
	targetConn, err := pgx.Connect(targetConnConfig)
	if err != nil {
		fmt.Printf("Unable to establish target SQL connection: %v\n", err)
		return
	}
	defer targetConn.Close()

	// Disable synchronous commit to optimize performance on EBS-backed databases
	_, err = targetConn.Exec("SET synchronous_commit = off")
	if err != nil {
		fmt.Printf("Could not disable synchronous_commit: %s\n", err)
		return
	}

	// Disable Citus deadlock prevention to allow multi-shard modifications
	_, err = targetConn.Exec("SET citus.enable_deadlock_prevention = off")
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

	// Create replication slot with "skip-empty-xacts" option
	var snapshotName string
	snapshotName, err = replicationConn.CreateReplicationSlotAndGetSnapshotName(slotName, "test_decoding")
	if err != nil {
		fmt.Printf("replication slot create failed: %v\n", err)
		return
	}

	fmt.Printf("Truncating target tables\n")
	err = truncateTargetTables(targetConn, tables)
	if err != nil {
		fmt.Printf("Target truncate failed: %s\n", err)
		return
	}

	fmt.Printf("Making base backup\n")
	err = makeBaseBackup(sourceURL, targetURL, snapshotName)
	if err != nil {
		fmt.Printf("Base backup failed: %s\n", err)
		err = replicationConn.DropReplicationSlot(slotName)
		if err != nil {
			fmt.Printf("Error dropping replication slot: %s\n", err)
		}
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	stop, err := startReplication(slotName, replicationConn, targetConn, tables)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}

	s := <-sigs
	fmt.Printf("Received %s, dropping replication slot and exiting...\n", s)

	stop <- true

	_, err = sourceConn.Exec(fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))
	if err != nil {
		fmt.Printf("Error dropping replication slot: %s\n", err)
	}
}
