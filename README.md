# pg_warp [![Build Status](https://travis-ci.org/citusdata/pg_warp.svg?branch=master)](https://travis-ci.org/citusdata/pg_warp)

pg_warp makes it easy to logically replicate data from a source Postgres instance with the [test_decoding](https://www.postgresql.org/docs/9.6/static/test-decoding.html) logical decoding plugin to a target Postgres instance, including distributed Postgres running on Citus Cloud.

pg_warp first takes an initial backup using standard Postgres tooling, and then consistently replays any changes that were made after the consistent snapshot used for the initial backup.

Contrary to other logical replication solutions, this test_decoding-based approach has little overhead on the source database, and resembles the native logical replication feature in PostgreSQL 10.

The necessary test_decoding plugin has been part of PostgreSQL since version 9.4 and is considered stable and safe to use on production systems.

Through the use of [replication slots](https://www.postgresql.org/docs/9.6/static/warm-standby.html#STREAMING-REPLICATION-SLOTS) and [replication origins](https://www.postgresql.org/docs/9.6/static/replication-origins.html) pg_warp is able to recover from crashes of the replication tool itself, by simply starting the command up again with the same source and destination instance.

Unlike some other solutions, pg_warp has no restrictions or issues with the use of complex data types, or large JSON, JSONB or hstore columns.

## Usage

First, consider access restrictions and decide where you want to run pg_warp:

* **A)** Run pg_warp on the destination system directly (best replication throughput, requires giving source credentials and firewall access to the destination system)
* **B)** Run pg_warp on a temporary server inside the source VPC (easier to setup, lower replication throughput)

For both of these choices you will need to download the latest [pg_warp binary](https://github.com/citusdata/pg_warp/releases) and then run pg_warp as following:

```
pg_warp --source postgres://username:password@hostname:port/database --destination postgres://username:password@hostname:port/database
```

By default pg_warp will copy all tables that exist on the source, and assume that their schema has already been created on the destination.

It will also issue TRUNCATE for every of those tables on the destination system - use with care!

After the initial backup has completed pg_warp will automatically switch into ongoing replication mode. In case the process crashes or is stopped, just start it again with the same parameters.

## Parallelism for the initial backup

Its recommended to add the following options for most real-world use cases:

```
  --parallel-dump=value
    	Number of parallel operations whilst dumping the initial sync data (default 1 = not parallel) (default 1)
  --parallel-restore=value
    	Number of parallel operations whilst restoring the initial sync data (default 1 = not parallel) (default 1)
  --tmp-dir=string
    	Directory where we store temporary files as part of parallel operations (this needs to be fast and have space for your full data set)
```

For parallel operations you need to specify a **temporary directory that has enough space for your full data set** (size means records only without indexes), and ideally resides on a fast disk. This directory is used to first download the data in parallel, and then restore in parallel to the destination system.

## Limiting the tables that are replicated

It is often useful to limit the tables that should be copied and replicated. To do this pg_warp supports the same options as pg_dump:

```
  -t, --table=value
    	dump, restore and replicate the named table(s) only
  -T, --exclude-table=value
    	do NOT dump, restore and replicate the named table(s)
```

Note that you currently need to fully qualify the names (= include the schema), e.g. `-t public.my_data` will only copy and replicate the data in the my_data table in the public schema.

## Required Postgres configuration on the source

The following configuration is required to enable logical decoding and replication slots:

```
wal_level = logical
max_replication_slots = 5 # has to be > 0
max_wal_senders = 5 # has to be > 0
```

## Required Postgres configuration on the target

The following configuration is required to enable replication origins on the target:

```
max_replication_slots = 5 # has to be > 0
```

We also recommend considering the use of `synchronous_commit = off` to speed up writes and increase the replication throughput. This does not lead to the potential of data loss on behalf of pg_warp itself, although other connections might be affected.

## Cleaning the replication slot (IMPORTANT!)

After you are finished using and/or testing pg_warp, run it once with the `--clean` option, with the source and destination URLs specified.

This will remove the replication origin on the destination, and more importantly, the replication slot on the source.

**If you do not remove the replication slot on the source your WAL will grow forever until you run out of disk space!** (running pg_warp consumes the WAL, so this only applies once you stop using pg_warp)

## Things to be aware of

* The initial backup has some I/O impact on the source instance due to the data that needs to be read - we recommend kicking off the process in a time period with lower activity, and to always test on a copy or staging system first
* All tables that receive UPDATE/DELETE commands need to have a primary key set (otherwise these operations will be ignored for ongoing replication - INSERTs work fine without primary key)
* Sequence values will not be updated on the destination (except for the initial sync), you'll need to update these manually before you insert something from your application into the destination system
* DDL is currently not replicated (for example from schema migrations)

## Initial Authors

* [Lukas Fittl](https://github.com/LukasFittl)
* [Andres Freund](https://github.com/anarazel)

## License

Copyright 2017 Citus Data Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
