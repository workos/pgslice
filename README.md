# pgslice

Postgres partitioning as easy as pie. Works great for both new and existing tables, with zero downtime and minimal app changes. No need to install anything on your database server. Archive older data on a rolling basis to keep your database size under control.

**Note:** This is a TypeScript port of the [original Ruby-based pgslice](https://github.com/ankane/pgslice).

## Install

pgslice is a command line tool. Requires Node.js 20+.

To install globally:

```sh
npm install -g pgslice
```

Or run directly with npx:

```sh
npx pgslice <command>
```

This will give you the `pgslice` command.

## Global Options

All commands support these global options:

- `--url`: Database URL (can also be set via `PGSLICE_URL` environment variable)
- `--dry-run`: Print SQL statements without executing them

## Steps

1. Ensure the table you want to partition has been created. We’ll refer to this as `<table>`.

2. Specify your database credentials

```sh
export PGSLICE_URL=postgres://localhost/myapp_development
```

3. Create an intermediate table

```sh
pgslice prep <table> <column> <period>
```

The column should be a `timestamp`, `timestamptz`, or `date` column and period can be `day`, `month`, or `year`.

This creates a partitioned table named `<table>_intermediate` using range partitioning.

Options:

- `--no-partition`: Create a non-partitioned intermediate table (useful for one-off tasks)

4. Add partitions to the intermediate table

```sh
pgslice add_partitions <table> --intermediate --past 3 --future 3
```

Use the `--past` and `--future` options to control the number of partitions.

5. Enable mirroring triggers for live data changes

```sh
pgslice enable_mirroring <table>
```

This enables triggers that automatically mirror INSERT, UPDATE, and DELETE operations from the original table to the intermediate table during the partitioning process. This ensures that any data changes made after you start the partitioning process are captured in both tables.

6. _Optional, for tables with data_ - Fill the partitions in batches with data from the original table

```sh
pgslice fill <table>
```

Options:

- `--batch-size`: Number of rows per batch (default: `10000`)
- `--sleep`: Seconds to sleep between batches (default: `0`)
- `--swapped`: Fill from retired table to partitioned table (after swap)
- `--source-table`: Source table name (default: original table or retired table if `--swapped`)
- `--dest-table`: Destination table name (default: intermediate table or partitioned table if `--swapped`)
- `--start`: Primary key value to start from (numeric or ULID)
- `--where`: Additional WHERE conditions to filter rows

By default, `fill` auto-detects a starting ID by scanning for the smallest
primary key (bounded to the partition time range for partitioned tables). This
lookup can be slow on very large tables without a supporting index. If you're
resuming or want to skip the startup scan, pass `--start` instead (the batch
output includes `endId` for easy resuming).

To sync data across different databases, check out [pgsync](https://github.com/ankane/pgsync).

7. Analyze tables

```sh
pgslice analyze <table>
```

Options:

- `--swapped`: Analyze the partitioned table (after swap)

8. Sync/Validate the tables

This will ensure the two tables are definitely in sync. It should be a no-op, but will generate
INSERT, UPDATE, and DELETE statements if discrepencies are discovered. On a production system,
ensure you understand the `--window-size`, `--delay`, and `--delay-multiplier` options.

```sh
pgslice synchronize <table> [options]
```

Options:

- `--source-table`: Source table to compare (default: `<table>`)
- `--target-table`: Target table to compare (default: `<table>_intermediate`)
- `--primary-key`: Primary key column name (default: detected from table)
- `--start`: Primary key value to start synchronization at
- `--window-size`: Number of rows to synchronize per batch (default: `1000`)
- `--delay`: Base delay in seconds between batches (default: `0`)
- `--delay-multiplier`: Delay multiplier for batch time (default: `0`)
- `--dry-run`: Print statements without executing

9. Swap the intermediate table with the original table

```sh
pgslice swap <table>
```

The original table is renamed `<table>_retired` and the intermediate table is renamed `<table>`.

Options:

- `--lock-timeout`: Lock timeout for the swap operation (default: `5s`)

10. Disable mirroring triggers

```sh
pgslice disable_mirroring <table>
```

After the swap, the original mirroring triggers are no longer needed since the tables have been swapped.

11. Enable Reverse Mirroring (now-partitioned table to retired table)

This will make unswapping later less problematic as the two tables are kept in sync. Note that
the tables will be slightly out of sync. Find some ID from before the swap, and run the table
synchronize commands from Step 8 on the table to be sure to catch those rows.

```sh
pgslice enable_retired_mirroring <table>  # undo with pgslice disable_retired_mirroring <table>
```

12. Fill the rest (rows inserted between the first fill and the swap)

This step should not be needed if you did the pgslice synchronize in step 8.

```sh
pgslice fill <table> --swapped
```

13. Disable retired mirroring triggers

```sh
pgslice disable_retired_mirroring <table>
```

Once you're confident the retired table is no longer needed and you're ready to drop it, disable the retired mirroring triggers.

14. Back up the retired table with a tool like [pg_dump](https://www.postgresql.org/docs/current/static/app-pgdump.html) and drop it

```sql
pg_dump -c -Fc -t <table>_retired $PGSLICE_URL > <table>_retired.dump
psql -c "DROP TABLE <table>_retired" $PGSLICE_URL
```

## Sample Output

pgslice prints the SQL commands that were executed on the server. To print without executing, use the `--dry-run` option.

```sh
pgslice prep visits created_at month
```

```sql
BEGIN;

CREATE TABLE "public"."visits_intermediate" (LIKE "public"."visits" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS INCLUDING STATISTICS INCLUDING GENERATED INCLUDING COMPRESSION) PARTITION BY RANGE ("created_at");

CREATE INDEX ON "public"."visits_intermediate" USING btree ("created_at");

COMMENT ON TABLE "public"."visits_intermediate" is 'column:created_at,period:month,cast:date,version:3';

COMMIT;
```

```sh
pgslice add_partitions visits --intermediate --past 1 --future 1
```

```sql
BEGIN;

CREATE TABLE "public"."visits_202408" PARTITION OF "public"."visits_intermediate" FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');

ALTER TABLE "public"."visits_202408" ADD PRIMARY KEY ("id");

CREATE TABLE "public"."visits_202409" PARTITION OF "public"."visits_intermediate" FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');

ALTER TABLE "public"."visits_202409" ADD PRIMARY KEY ("id");

CREATE TABLE "public"."visits_202410" PARTITION OF "public"."visits_intermediate" FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');

ALTER TABLE "public"."visits_202410" ADD PRIMARY KEY ("id");

COMMIT;
```

```sh
pgslice fill visits
```

```sql
/* 1 of 3 */
INSERT INTO "public"."visits_intermediate" ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM "public"."visits"
    WHERE "id" > 0 AND "id" <= 10000 AND "created_at" >= '2024-08-01'::date AND "created_at" < '2024-11-01'::date

/* 2 of 3 */
INSERT INTO "public"."visits_intermediate" ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM "public"."visits"
    WHERE "id" > 10000 AND "id" <= 20000 AND "created_at" >= '2024-08-01'::date AND "created_at" < '2024-11-01'::date

/* 3 of 3 */
INSERT INTO "public"."visits_intermediate" ("id", "user_id", "ip", "created_at")
    SELECT "id", "user_id", "ip", "created_at" FROM "public"."visits"
    WHERE "id" > 20000 AND "id" <= 30000 AND "created_at" >= '2024-08-01'::date AND "created_at" < '2024-11-01'::date
```

```sh
pgslice analyze visits
```

```sql
ANALYZE VERBOSE "public"."visits_202408";

ANALYZE VERBOSE "public"."visits_202409";

ANALYZE VERBOSE "public"."visits_202410";

ANALYZE VERBOSE "public"."visits_intermediate";
```

```sh
pgslice swap visits
```

```sql
BEGIN;

SET LOCAL lock_timeout = '5s';

ALTER TABLE "public"."visits" RENAME TO "visits_retired";

ALTER TABLE "public"."visits_intermediate" RENAME TO "visits";

ALTER SEQUENCE "public"."visits_id_seq" OWNED BY "public"."visits"."id";

COMMIT;
```

## Adding Partitions

To add partitions, use:

```sh
pgslice add_partitions <table> --future 3
```

Add this as a cron job to create a new partition each day, month, or year.

```sh
# day
0 0 * * * pgslice add_partitions <table> --future 3 --url ...

# month
0 0 1 * * pgslice add_partitions <table> --future 3 --url ...

# year
0 0 1 1 * pgslice add_partitions <table> --future 3 --url ...
```

Add a monitor to ensure partitions are being created.

```sql
SELECT 1 FROM
    pg_catalog.pg_class c
INNER JOIN
    pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE
    c.relkind = 'r' AND
    n.nspname = 'public' AND
    c.relname = '<table>_' || to_char(NOW() + INTERVAL '3 days', 'YYYYMMDD')
    -- for months, use to_char(NOW() + INTERVAL '3 months', 'YYYYMM')
    -- for years, use to_char(NOW() + INTERVAL '3 years', 'YYYY')
```

## Archiving Partitions

Back up and drop older partitions each day, month, or year.

```sh
pg_dump -c -Fc -t <table>_202409 $PGSLICE_URL > <table>_202409.dump
psql -c "DROP TABLE <table>_202409" $PGSLICE_URL
```

If you use [Amazon S3](https://aws.amazon.com/s3/) for backups, [s3cmd](https://github.com/s3tools/s3cmd) is a nice tool.

```sh
s3cmd put <table>_202409.dump s3://<s3-bucket>/<table>_202409.dump
```

## Schema Updates

Once a table is partitioned, make schema updates on the master table only (not partitions). This includes adding, removing, and modifying columns, as well as adding and removing indexes and foreign keys.

## Additional Commands

To undo prep (which will delete partitions), use:

```sh
pgslice unprep <table>
```

To undo swap, use:

```sh
pgslice unswap <table>
```

To enable mirroring triggers for live data changes during partitioning (before swap), use:

```sh
pgslice enable_mirroring <table>
```

To disable mirroring triggers after partitioning is complete, use:

```sh
pgslice disable_mirroring <table>
```

To show the version, use:

```sh
pgslice version
# or
pgslice --version
```

## Additional Options

Set the tablespace when adding partitions

```sh
pgslice add_partitions <table> --tablespace fastspace
```

## App Considerations

This set up allows you to read and write with the original table name with no knowledge it’s partitioned. However, there are a few things to be aware of.

### Reads

When possible, queries should include the column you partition on to limit the number of partitions the database needs to check. For instance, if you partition on `created_at`, try to include it in queries:

```sql
SELECT * FROM
    visits
WHERE
    user_id = 123 AND
    -- for performance
    created_at >= '2024-09-01' AND created_at < '2024-09-02'
```

For this to be effective, ensure `constraint_exclusion` is set to `partition` (the default value) or `on`.

```sql
SHOW constraint_exclusion;
```

## One Off Tasks

You can also use pgslice to reduce the size of a table without partitioning by creating a new table, filling it with a subset of records, and swapping it in.

```sh
pgslice prep <table> --no-partition
pgslice fill <table> --where "id > 1000" # use any conditions
pgslice analyze <table>
pgslice swap <table>
pgslice fill <table> --where "id > 1000" --swapped
```

## Triggers

Triggers aren’t copied from the original table. You can set up triggers on the intermediate table if needed.

## Data Protection

Always make sure your [connection is secure](https://ankane.org/postgres-sslmode-explained) when connecting to a database over a network you don’t fully trust. Your best option is to connect over SSH or a VPN. Another option is to use `sslmode=verify-full`. If you don’t do this, your database credentials can be compromised.

## Reference

- [PostgreSQL Manual](https://www.postgresql.org/docs/current/static/ddl-partitioning.html)

## Related Projects

Also check out:

- [Dexter](https://github.com/ankane/dexter) - The automatic indexer for Postgres
- [PgHero](https://github.com/ankane/pghero) - A performance dashboard for Postgres
- [pgsync](https://github.com/ankane/pgsync) - Sync Postgres data to your local machine

## History

View the [changelog](https://github.com/ankane/pgslice/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgslice/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgslice/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/pgslice.git
cd pgslice
npm install
npm run build
npm test
```

To format code:

```sh
npm run format
```

To test against different versions of Postgres with Docker, use:

```sh
docker run -p=8000:5432 postgres:16
PGSLICE_URL=postgres://postgres@localhost:8000/postgres npm test
```

On Mac, you must use [Docker Desktop](https://www.docker.com/products/docker-desktop/) for the port mapping to localhost to work.
