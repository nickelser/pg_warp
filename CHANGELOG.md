# Changelog

## 0.5.0      2017-08-11

* Add optional statement and transaction batching
  - This is experimental right now but has been used successfully, and can be
    particularly helpful if you are seeing replication lag issues
  - Transaction batching will merge multiple smaller transactions together into
    one large one (this works because pg_warp is a single writer), to avoid
    roundtrip costs of BEGIN/COMMIT
  - Statement batching will merge multiple statements in the same transaction
    together into a multi-statement string, in order to save on roundtrip times
    otherwise incurred with sending individual statements and waiting for their
    results
  - Both statement and transaction batching are turned off by default


## 0.4.1      2017-08-03

* Don't require target objects to exist when syncing schema
* Create extensions on destination system when syncing schema
* Allow source/destination connection to be made without TLS
* Ignore operator families when doing a full initial backup including schema


## 0.4.0      2017-07-21

* Apply multi-shard deletes on target using SELECT+DELETE
* Show replication lag based on pg_current_xlog_location() always (to fix 9.4/9.5)


## 0.3.1      2017-07-20

* Show log message when replication origin is being created


## 0.3.0      2017-07-19

* Handle distributed tables that have different primary keys than the source


## 0.2.0      2017-07-18

* Run truncate with all tables at once, to avoid problems with foreign keys
* Add --sync-schema option to perform a full restore
* Cleanup error messages and include timestamp in progress reporting


## 0.1.4      2017-07-18

* Support Postgres versions before 9.6


## 0.1.3      2017-07-13

* Fix issue with running dump/restore in parallel mode


## 0.1.2      2017-07-11

* Improve test_decoding consumer
  - Cover additional edge cases
  - Output separate UPDATE SET clauses instead of using (..) = (..) syntax


## 0.1.1      2017-07-11

* Add support for array types in test_decoding messages
* Abort in the case of parser errors of test_decoding messages (and output message)


## 0.1.0      2017-07-11

* Initial release
