# pg_warp [![Build Status](https://travis-ci.org/citusdata/pg_warp.svg?branch=master)](https://travis-ci.org/citusdata/pg_warp)

pg_warp makes it easy to logically replicate data from a source Postgres instance with the [test_decoding](https://www.postgresql.org/docs/9.6/static/test-decoding.html) logical decoding plugin to a target Postgres instance, including distributed Postgres running on Citus Cloud.

Contrary to other logical replication solutions, this test_decoding-based approach has little overhead on the source database, and resembles the logical replication that PostgreSQL 10 will include. The necessary test_decoding plugin has been part of PostgreSQL since version 9.4 and is considered stable and safe to use on production systems.

It first takes a base backup using standard Postgres tooling, and then consistently replays any changes that were made after the consistent snapshot used for the base backup.

## Usage

First, consider access restrictions and decide where you want to run pg_warp:

* **A)** Run pg_warp on the destination system directly (best replication throughput, requires giving source credentials and firewall access to the destination system)
* **B)** Run pg_warp on a temporary server inside the source VPC (easier to setup, lower replication throughput)

For both of these choices you will need to download the latest [pg_warp binary](https://github.com/citusdata/pg_warp/releases) and make sure you have enough disk space available to hold your database data.

The basic mode of operation only requires the source postgres:// URL and a destination postgres:// URL:

```
pg_warp --source postgres://username:password@hostname:port/database --destination postgres://username:password@hostname:port/database
```

By default pg_warp will copy all tables that exist on the source, and assume that their schema has already been created on the destination. It will also issue TRUNCATE for every of those tables on the destination system - use with care!

After the initial base backup has completed pg_warp will automatically switch into ongoing replication mode. In case the process crashes or is stopped, just start it again with the same parameters.

## Parallelism for the base backup

Its recommended to add the following options for most real-world use cases:

```
  --parallel-dump=value
    	Number of parallel operations whilst dumping the initial sync data (default 1 = not parallel) (default 1)
  --parallel-restore=value
    	Number of parallel operations whilst restoring the initial sync data (default 1 = not parallel) (default 1)
  --tmp-dir=string
    	Directory where we store temporary files (e.g. for parallel operations), this needs to be fast and have space for your full data set
```

Note that the temporary directory is necessary since in parallel mode the data is downloaded first, and then restored as a separate step. The directory ideally needs to be on a fast disk, and the disk needs to have enough space to fit all your data (logically, excluding indices).

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
max_replication_slots = 5
```

## Required Postgres configuration on the target

The following configuration is required to enable replication origins on the target:

```
max_replication_slots = 5
```

We also recommend considering the use of `synchronous_commit = off` to speed up writes and increase the replication throughput.

## Cleaning the replication slot (IMPORTANT!)

After you are finished using and/or testing pg_warp, run it once with the `--clean` option, as well as the source and destination URLs specified.

This will remove the replication origin on the destination, and more importantly, the replication slot on the source.

**If you do not remove the replication slot on the source your WAL will grow forever until you run out of disk space!** (running pg_warp consumes the WAL, so this only applies once you stop using pg_warp)

## Things to be aware of

* The initial base backup has some I/O impact on the source instance due to the data that needs to be read - we recommend kicking off the process in a time period with lower activity, and to always test on a copy or staging system first
* All tables that receive UPDATE/DELETE commands need to have a primary key set (otherwise these operations will be ignored for ongoing replication - INSERTs work fine without primary key)
* Sequence values will not be updated on the destination (except for the initial sync), you'll need to update these manually before you insert something from your application into the destination system
* DDL is currently not replicated (for example from schema migrations)

## Initial Authors

* [Lukas Fittl](https://github.com/LukasFittl)
* [Andres Freund](https://github.com/anarazel)

## License

Licensed under the MIT license<br/>
Copyright (c) 2017, Citus Data Inc.
