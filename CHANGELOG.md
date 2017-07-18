# Changelog

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
