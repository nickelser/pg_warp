# Changelog

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
