import {
  CommonQueryMethods,
  createPool,
  DatabasePoolConnection,
  DatabaseTransactionConnection,
  type DatabasePool,
} from "slonik";
import { createQueryLoggingInterceptor } from "slonik-interceptor-query-logging";

import { z } from "zod";

import type {
  AddPartitionsOptions,
  AnalyzeOptions,
  ColumnInfo,
  DisableMirroringOptions,
  EnableMirroringOptions,
  FillBatchResult,
  FillOptions,
  MaintainOptions,
  MaintainResult,
  PartitionModel,
  Period,
  PrepOptions,
  StatusOptions,
  SwapOptions,
  SynchronizeBatchResult,
  SynchronizeOptions,
  TableStatus,
  UnprepOptions,
  UnswapOptions,
} from "./types.js";
import { isPeriod } from "./types.js";
import { TableSettings } from "./table-settings.js";
import { Table, getServerVersionNum, type TableGrant } from "./table.js";
import {
  DateRanges,
  advanceDate,
  extendRanges,
  extendRangesBackward,
  maxUpperBound,
  minLowerBound,
  rangeOverlaps,
  roundDate,
  type DateRange,
} from "./date-ranges.js";
import { formatDateForSql, rawSql, sql } from "./sql-utils.js";
import { Mirroring } from "./mirroring.js";
import { Filler } from "./filler.js";
import { Synchronizer } from "./synchronizer.js";
import { Swapper } from "./swapper.js";
import { AdvisoryLock } from "./advisory-lock.js";

/**
 * Table privileges pgslice knows how to re-issue on new partitions, mapped to
 * their SQL keyword. Privilege names come from `aclexplode`; anything not in
 * this allow-list is skipped so we never emit an unrecognized keyword.
 */
const GRANTABLE_PRIVILEGES: Record<string, ReturnType<typeof sql.fragment>> = {
  SELECT: sql.fragment`SELECT`,
  INSERT: sql.fragment`INSERT`,
  UPDATE: sql.fragment`UPDATE`,
  DELETE: sql.fragment`DELETE`,
  TRUNCATE: sql.fragment`TRUNCATE`,
  REFERENCES: sql.fragment`REFERENCES`,
  TRIGGER: sql.fragment`TRIGGER`,
};

interface PgsliceOptions {
  /**
   * Whether to use Postgres advisory locks to prevent concurrent operations
   * on the same table for the same operation. Defaults to true.
   */
  advisoryLocks?: boolean;
}

export class Pgslice {
  #pool: DatabasePool | null = null;
  #advisoryLocks: boolean;

  constructor(pool: DatabasePool, options: PgsliceOptions) {
    this.#advisoryLocks = options.advisoryLocks ?? true;
    this.#pool = pool;
  }

  static async connect(
    databaseUrl: URL,
    options: PgsliceOptions = {},
  ): Promise<Pgslice> {
    const url = new URL(databaseUrl.toString());

    if (!url.searchParams.has("application_name")) {
      url.searchParams.set("application_name", "pgslice");
    }

    const pool = await createPool(url.toString(), {
      // We don't want to perform any operations in parallel, and should
      // only ever need a single connection at a time.
      maximumPoolSize: 1,

      // Never retry queries.
      queryRetryLimit: 0,

      interceptors: [createQueryLoggingInterceptor()],
    });
    const instance = new Pgslice(pool, options);
    return instance;
  }

  private get pool() {
    if (!this.#pool) {
      throw new Error("Not connected to the database");
    }

    return this.#pool;
  }

  async start<T>(
    handler: (transaction: DatabasePoolConnection) => Promise<T>,
  ): Promise<T> {
    return this.pool.connect(handler);
  }

  async #withLock<T>(
    tx: CommonQueryMethods,
    table: Table,
    operation: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    if (!this.#advisoryLocks) {
      return handler();
    }
    return AdvisoryLock.withLock(tx, table, operation, handler);
  }

  async #acquireLock(
    connection: CommonQueryMethods,
    table: Table,
    operation: string,
  ): Promise<() => Promise<void>> {
    if (!this.#advisoryLocks) {
      return async () => {};
    }
    return AdvisoryLock.acquire(connection, table, operation);
  }

  async close(): Promise<void> {
    if (this.#pool) {
      if ("end" in this.#pool) {
        await this.#pool.end();
      }
      this.#pool = null;
    }
  }

  /**
   * Creates an intermediate table for partitioning.
   *
   * This is the first step in the pgslice workflow. The intermediate table
   * can be created as a partitioned table (the default) or as a regular table
   * with `partition: false`.
   */
  async prep(
    connection: DatabasePoolConnection,
    options: PrepOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "prep", async () => {
        const intermediate = table.intermediate;

        if (!(await table.exists(tx))) {
          throw new Error(`Table not found: ${table.toString()}`);
        }

        if (await intermediate.exists(tx)) {
          throw new Error(`Table already exists: ${intermediate.toString()}`);
        }

        if (options.partition) {
          const columns = await table.columns(tx);
          const columnInfo = columns.find((c) => c.name === options.column);
          if (!columnInfo) {
            throw new Error(`Column not found: ${options.column}`);
          }

          if (!isPeriod(options.period)) {
            throw new Error(`Invalid period: ${options.period}`);
          }

          await this.#createPartitionedIntermediateTable(
            tx,
            table,
            intermediate,
            columnInfo,
            options.period,
          );
        } else {
          await this.#createUnpartitionedIntermediateTable(
            tx,
            table,
            intermediate,
          );
        }
      }),
    );
  }

  async #createPartitionedIntermediateTable(
    tx: DatabaseTransactionConnection,
    table: Table,
    intermediate: Table,
    columnInfo: ColumnInfo,
    period: Period,
  ): Promise<void> {
    const serverVersionNum = await getServerVersionNum(tx);

    // Create partitioned table using the appropriate INCLUDING clauses
    const intermediateIdent = intermediate.sqlIdentifier;
    const tableIdent = table.sqlIdentifier;
    const columnIdent = sql.identifier([columnInfo.name]);

    const includings = [
      sql.fragment`COMMENTS`,
      sql.fragment`CONSTRAINTS`,
      sql.fragment`DEFAULTS`,
      sql.fragment`STORAGE`,
      sql.fragment`STATISTICS`,
      sql.fragment`GENERATED`,
    ];

    // For Postgres 14+, include COMPRESSION
    if (serverVersionNum >= 140000) {
      includings.push(sql.fragment`COMPRESSION`);
    }

    await tx.query(
      sql.typeAlias("void")`
        CREATE TABLE ${intermediateIdent} (LIKE ${tableIdent}
          INCLUDING ${sql.join(includings, sql.fragment` INCLUDING `)}
        ) PARTITION BY RANGE (${columnIdent})
      `,
    );

    // Copy indexes
    for (const indexDef of await table.indexDefs(tx)) {
      // Transform the index definition to point to the intermediate table
      const transformedIndexDef = indexDef
        .replace(/ ON \S+ USING /, ` ON ${intermediate.quoted} USING `)
        .replace(/ INDEX .+ ON /, " INDEX ON ");
      await tx.query(sql.typeAlias("void")`${rawSql(transformedIndexDef)}`);
    }

    // Copy foreign keys
    await this.#copyForeignKeys(tx, table, intermediate);

    // Add metadata comment - use cast from columnInfo, default to 'date' if not a timestamp type
    const cast = columnInfo.cast ?? "date";
    const comment = `column:${columnInfo.name},period:${period},cast:${cast},version:3`;
    await tx.query(
      sql.typeAlias("void")`
        COMMENT ON TABLE ${intermediateIdent} IS ${sql.literalValue(comment)}
      `,
    );
  }

  async #createUnpartitionedIntermediateTable(
    tx: DatabaseTransactionConnection,
    table: Table,
    intermediate: Table,
  ): Promise<void> {
    // Create table with all properties
    await tx.query(
      sql.typeAlias("void")`
        CREATE TABLE ${intermediate.sqlIdentifier} (LIKE ${table.sqlIdentifier} INCLUDING ALL)
      `,
    );

    // Copy foreign keys (not included with LIKE ... INCLUDING ALL)
    await this.#copyForeignKeys(tx, table, intermediate);
  }

  async #copyForeignKeys(
    tx: DatabaseTransactionConnection,
    source: Table,
    target: Table,
  ): Promise<void> {
    for (const fkDef of await source.foreignKeys(tx)) {
      await tx.query(
        sql.typeAlias(
          "void",
        )`ALTER TABLE ${target.sqlIdentifier} ADD ${rawSql(fkDef)}`,
      );
    }
  }

  /**
   * Adds partitions to a partitioned table.
   *
   * Returns the names of the partitions that were created (empty when every
   * target period is already covered, which keeps re-runs idempotent).
   */
  async addPartitions(
    connection: DatabasePoolConnection,
    options: AddPartitionsOptions,
  ): Promise<string[]> {
    const originalTable = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, originalTable, "add_partitions", async () => {
        // Pin the transaction to UTC. Partition boundaries are UTC calendar
        // dates; without this, reading an existing timestamptz bound via
        // pg_get_expr renders it in the session timezone (so the parsed
        // boundary day drifts) and emitting a date/timestamptz literal coerces
        // it through the session timezone — either of which misaligns the new
        // partitions against the existing ones under a non-UTC session.
        await tx.query(sql.typeAlias("void")`SET LOCAL TIME ZONE 'UTC'`);

        const targetTable = options.intermediate
          ? originalTable.intermediate
          : originalTable;

        if (!(await targetTable.exists(tx))) {
          throw new Error(`Table not found: ${targetTable.toString()}`);
        }

        const settings = await targetTable.fetchSettings(tx);
        if (!settings) {
          let message = `No settings found: ${targetTable.toString()}`;
          if (!options.intermediate) {
            message += "\nDid you mean to use --intermediate?";
          }
          throw new Error(message);
        }

        const past = options.past ?? 0;
        const future = options.future ?? 0;

        // Determine which table to read the schema (primary key) from.
        // For intermediate tables, use the original table.
        // For swapped tables, use the last existing partition (if any) or the original.
        let schemaTable: Table;
        if (options.intermediate) {
          schemaTable = originalTable;
        } else {
          const existingPartitions = await targetTable.partitions(tx);
          schemaTable =
            existingPartitions.length > 0
              ? existingPartitions[existingPartitions.length - 1]
              : originalTable;
        }

        // If the partitioned parent owns a primary key, Postgres propagates it
        // (and any partitioned indexes) to each new partition automatically, so
        // we must not add a per-partition primary key. Otherwise we follow the
        // classic pgslice model and add the key to each partition ourselves,
        // supporting composite keys.
        const parentPrimaryKey = await targetTable.primaryKeyColumns(tx);
        const partitionPrimaryKey =
          parentPrimaryKey.length > 0
            ? []
            : await this.#partitionPrimaryKeyColumns(tx, schemaTable);

        const grants =
          (options.inheritGrants ?? true) ? await targetTable.grants(tx) : [];

        // Read existing partition bounds (empty for the intermediate/prep
        // flow, which operates on a freshly-created intermediate table).
        const existingRanges = options.intermediate
          ? []
          : await targetTable.rangePartitionBounds(tx);
        const finiteRanges = existingRanges.filter((r) => !r.isDefault);

        let ranges: Iterable<DateRange>;
        if (finiteRanges.length === 0) {
          // Fresh table (or the prep/intermediate flow): generate calendar-
          // aligned ranges centered on today, the classic pgslice behavior.
          ranges = new DateRanges({ period: settings.period, past, future });
        } else {
          // Existing partitioned table: extend contiguously from the current
          // coverage by partition *bounds*, independent of the legacy naming
          // or week-alignment scheme. This recognizes partitions created
          // outside pgslice (so it never renames or collides with them) and
          // continues whatever scheme they use without a gap or overlap at the
          // boundary.
          const today = roundDate(new Date(), settings.period);
          const candidates: DateRange[] = [];

          const maxUpper = maxUpperBound(existingRanges);
          const unboundedAbove = finiteRanges.some((r) => r.upperUnbounded);
          if (maxUpper && !unboundedAbove && future > 0) {
            const horizon = advanceDate(today, settings.period, future);
            for (const range of extendRanges({
              anchorStart: maxUpper,
              period: settings.period,
              horizon,
            })) {
              candidates.push(range);
            }
          }

          const minLower = minLowerBound(existingRanges);
          const unboundedBelow = finiteRanges.some((r) => r.lowerUnbounded);
          if (minLower && !unboundedBelow && past > 0) {
            const horizon = advanceDate(today, settings.period, -past);
            for (const range of extendRangesBackward({
              anchorEnd: minLower,
              period: settings.period,
              horizon,
            })) {
              candidates.push(range);
            }
          }

          // Defensive: drop any candidate that would overlap existing coverage.
          // Anchored generation shouldn't produce one, but this keeps re-runs
          // idempotent and guards against an unexpected existing layout.
          ranges = candidates.filter(
            (c) =>
              !existingRanges.some((r) => rangeOverlaps(c.start, c.end, r)),
          );
        }

        const created: string[] = [];

        for (const range of ranges) {
          const partitionTable = originalTable.partition(range.suffix);

          if (await partitionTable.exists(tx)) {
            continue;
          }

          const startDate = formatDateForSql(range.start, settings.cast);
          const endDate = formatDateForSql(range.end, settings.cast);

          // Build the CREATE TABLE statement
          let createSql = sql.fragment`
          CREATE TABLE ${partitionTable.sqlIdentifier}
          PARTITION OF ${targetTable.sqlIdentifier}
          FOR VALUES FROM (${startDate}) TO (${endDate})
        `;

          if (options.tablespace) {
            createSql = sql.fragment`${createSql} TABLESPACE ${sql.identifier([options.tablespace])}`;
          }

          await tx.query(sql.typeAlias("void")`${createSql}`);

          if (partitionPrimaryKey.length > 0) {
            await tx.query(
              sql.typeAlias("void")`
              ALTER TABLE ${partitionTable.sqlIdentifier}
              ADD PRIMARY KEY (${sql.join(
                partitionPrimaryKey.map((col) => sql.identifier([col])),
                sql.fragment`, `,
              )})
            `,
            );
          }

          for (const grant of grants) {
            await this.#applyGrant(tx, partitionTable, grant);
          }

          created.push(partitionTable.name);
        }

        return created;
      }),
    );
  }

  /**
   * Maintains every managed partitioned table the connection can see: each
   * partitioned parent (`relkind = 'p'`) carrying a valid pgslice settings
   * comment is discovered from the catalog and extended via {@link addPartitions}.
   * New partitioned tables are therefore picked up automatically with no
   * per-table configuration.
   *
   * The native-vs-pgslice distinction is handled by {@link addPartitions}: it
   * skips the per-partition primary key when the parent owns one (the inherited
   * composite key and any partitioned indexes propagate automatically) and adds
   * a per-partition key otherwise. New partitions are created with the default
   * replica identity, so each leaf's row identity is its own (or inherited)
   * primary key and no replica-identity DDL is required. After extending each
   * table this verifies every leaf has a usable replica identity, surfacing a
   * misconfigured table rather than silently shipping a CDC-unsafe partition.
   *
   * Each table is maintained independently: a failure on one table (for
   * example a non-empty DEFAULT partition that blocks creating the next one) is
   * recorded on that table's result and does not stop the rest of the fleet.
   */
  async maintain(
    connection: DatabasePoolConnection,
    options: MaintainOptions,
  ): Promise<MaintainResult[]> {
    const past = options.past ?? 0;
    const future = options.future ?? 0;

    const managed = await this.#discoverManagedTables(
      connection,
      options.schema,
    );

    const results: MaintainResult[] = [];
    for (const table of managed) {
      try {
        const partitionsCreated = await this.addPartitions(connection, {
          table: table.toString(),
          past,
          future,
          tablespace: options.tablespace,
          inheritGrants: options.inheritGrants,
        });

        const { model, partitionCount, unsafePartitions } =
          await connection.transaction(async (tx) => {
            const parentPrimaryKey = await table.primaryKeyColumns(tx);
            const partitions = await table.partitions(tx);
            const unsafe = await table.unsafeReplicaIdentityPartitions(tx);
            const model: PartitionModel =
              parentPrimaryKey.length > 0 ? "native" : "pgslice";
            return {
              model,
              partitionCount: partitions.length,
              unsafePartitions: unsafe,
            };
          });

        results.push({
          table: table.toString(),
          model,
          partitionsCreated,
          partitionCount,
          replicaIdentityReady: unsafePartitions.length === 0,
          unsafePartitions,
          error: null,
        });
      } catch (error) {
        results.push({
          table: table.toString(),
          model: null,
          partitionsCreated: [],
          partitionCount: 0,
          replicaIdentityReady: false,
          unsafePartitions: [],
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    return results;
  }

  /**
   * Discovers the managed partitioned parents visible to this connection: a
   * top-level partitioned table (`relkind = 'p'`, not itself a partition) whose
   * table comment parses as pgslice settings. Optionally restricted to a single
   * schema. The same `pg_partitioned_table`-style catalog scan that the runway
   * monitor uses, so scheduler and monitor share one source of truth.
   */
  async #discoverManagedTables(
    connection: DatabasePoolConnection,
    schema?: string,
  ): Promise<Table[]> {
    const schemaFilter = schema
      ? sql.fragment`AND n.nspname = ${schema}`
      : sql.fragment``;

    const rows = await connection.any(
      sql.type(
        z.object({
          schema: z.string(),
          name: z.string(),
          comment: z.string().nullable(),
        }),
      )`
        SELECT
          n.nspname AS schema,
          c.relname AS name,
          obj_description(c.oid, 'pg_class') AS comment
        FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'p'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema')
          AND NOT EXISTS (
            SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = c.oid
          )
          ${schemaFilter}
        ORDER BY n.nspname, c.relname
      `,
    );

    return rows.flatMap((row) => {
      if (!row.comment) {
        return [];
      }
      const settings = TableSettings.parseFromComment(row.comment);
      return settings ? [new Table(row.schema, row.name)] : [];
    });
  }

  /**
   * Resolves the primary key columns to place on each new partition in the
   * classic pgslice model (where the parent has no primary key of its own).
   * Supports composite keys and preserves the implicit single-column `id`
   * fallback of {@link Table.primaryKey}.
   */
  async #partitionPrimaryKeyColumns(
    tx: DatabaseTransactionConnection,
    schemaTable: Table,
  ): Promise<string[]> {
    const columns = await schemaTable.primaryKeyColumns(tx);
    if (columns.length > 0) {
      return columns;
    }
    return [await schemaTable.primaryKey(tx)];
  }

  /**
   * Re-issues a single grant from the parent table onto a new partition.
   * Unrecognized privileges are skipped rather than emitted unsafely.
   */
  async #applyGrant(
    tx: DatabaseTransactionConnection,
    table: Table,
    grant: TableGrant,
  ): Promise<void> {
    const privilege = GRANTABLE_PRIVILEGES[grant.privilege];
    if (!privilege) {
      return;
    }

    const grantee =
      grant.grantee === null
        ? sql.fragment`PUBLIC`
        : sql.fragment`${sql.identifier([grant.grantee])}`;
    const grantOption = grant.grantable
      ? sql.fragment` WITH GRANT OPTION`
      : sql.fragment``;

    await tx.query(
      sql.typeAlias("void")`
        GRANT ${privilege} ON TABLE ${table.sqlIdentifier} TO ${grantee}${grantOption}
      `,
    );
  }

  /**
   * Enables mirroring triggers from a table to its intermediate or retired table.
   * This ensures that INSERT, UPDATE, and DELETE operations on the source
   * table are automatically replicated to the target table.
   */
  async enableMirroring(
    connection: DatabasePoolConnection,
    options: EnableMirroringOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "enable_mirroring", async () => {
        const targetType = options.targetType ?? "intermediate";
        const target = table[targetType];

        if (!(await table.exists(tx))) {
          throw new Error(`Table not found: ${table.toString()}`);
        }
        if (!(await target.exists(tx))) {
          throw new Error(`Table not found: ${target.toString()}`);
        }

        await new Mirroring({ source: table, targetType }).enable(tx, target);
      }),
    );
  }

  /**
   * Disables mirroring triggers from a table to its intermediate or retired table.
   * This removes the triggers that were created by enableMirroring.
   */
  async disableMirroring(
    connection: DatabasePoolConnection,
    options: DisableMirroringOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "disable_mirroring", async () => {
        const targetType = options.targetType ?? "intermediate";

        if (!(await table.exists(tx))) {
          throw new Error(`Table not found: ${table.toString()}`);
        }

        await new Mirroring({ source: table, targetType }).disable(tx);
      }),
    );
  }

  /**
   * Fills the destination table from the source table in batches.
   * Each batch runs in its own transaction to allow resumable progress.
   *
   * @param options - Fill options including table names and batch configuration
   * @yields FillBatchResult after each batch is processed
   */
  async *fill(
    connection: DatabasePoolConnection,
    options: FillOptions,
  ): AsyncGenerator<FillBatchResult> {
    const releaseLock = await this.#acquireLock(
      connection,
      Table.parse(options.table),
      "fill",
    );

    try {
      const filler = await connection.transaction((tx) =>
        Filler.init(tx, options),
      );

      for await (const batch of filler.fill(connection)) {
        yield batch;
      }
    } finally {
      await releaseLock();
    }
  }

  /**
   * Synchronizes data between a source table and its intermediate table.
   * Detects and fixes discrepancies (missing, different, or extra rows).
   *
   * @param options - Synchronize options including table name and batch configuration
   * @yields SynchronizeBatchResult after each batch is processed
   */
  async *synchronize(
    connection: DatabasePoolConnection,
    options: SynchronizeOptions,
  ): AsyncGenerator<SynchronizeBatchResult> {
    const releaseLock = await this.#acquireLock(
      connection,
      Table.parse(options.table),
      "synchronize",
    );
    try {
      const synchronizer = await connection.transaction((tx) =>
        Synchronizer.init(tx, options),
      );

      for await (const batch of synchronizer.synchronize(connection)) {
        yield batch;
      }
    } finally {
      await releaseLock();
    }
  }

  /**
   * Swaps the intermediate table with the original table.
   *
   * This is the final step in the partitioning workflow. After the swap:
   * - The original table becomes `{table}_retired`
   * - The intermediate table becomes `{table}` (the main table)
   * - Sequence ownership is transferred to the new main table
   * - A retired mirroring trigger is enabled to keep the retired table in sync
   */
  async swap(
    connection: DatabasePoolConnection,
    options: SwapOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "swap", async () => {
        const swapper = new Swapper({
          table,
          direction: "forward",
          lockTimeout: options.lockTimeout,
        });
        await swapper.execute(tx);
      }),
    );
  }

  /**
   * Unswaps the retired table back to being the original table.
   *
   * This reverses a previous swap operation. After the unswap:
   * - The original table becomes `{table}_intermediate`
   * - The retired table becomes `{table}` (the main table)
   * - Sequence ownership is transferred to the new main table
   * - An intermediate mirroring trigger is enabled to keep the intermediate table in sync
   */
  async unswap(
    connection: DatabasePoolConnection,
    options: UnswapOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "unswap", async () => {
        const swapper = new Swapper({
          table,
          direction: "reverse",
          lockTimeout: options.lockTimeout,
        });
        await swapper.execute(tx);
      }),
    );
  }

  /**
   * Analyzes a table to update PostgreSQL statistics for query optimization.
   *
   * By default, analyzes the intermediate table. With `swapped: true`,
   * analyzes the main table after a swap operation.
   *
   * @returns The table that was analyzed
   */
  async analyze(options: AnalyzeOptions): Promise<Table> {
    const table = Table.parse(options.table);
    const targetTable = options.swapped ? table : table.intermediate;

    if (!(await targetTable.exists(this.pool))) {
      throw new Error(`Table not found: ${targetTable.toString()}`);
    }

    await this.pool.query(
      sql.typeAlias("void")`ANALYZE VERBOSE ${targetTable.sqlIdentifier}`,
    );

    return targetTable;
  }

  /**
   * Removes the intermediate table created by prep.
   *
   * This reverses the prep command by dropping the intermediate table
   * with CASCADE, which also removes any dependent objects like partitions.
   */
  async unprep(
    connection: DatabasePoolConnection,
    options: UnprepOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, table, "unprep", async () => {
        const intermediate = table.intermediate;

        if (!(await intermediate.exists(tx))) {
          throw new Error(`Table not found: ${intermediate.toString()}`);
        }

        await tx.query(
          sql.typeAlias("void")`
          DROP TABLE ${intermediate.sqlIdentifier} CASCADE
        `,
        );
      }),
    );
  }

  async status(options: StatusOptions): Promise<TableStatus> {
    const table = Table.parse(options.table);
    const intermediate = table.intermediate;

    const intermediateExists = await intermediate.exists(this.pool);

    // Check intermediate for partitions pre-swap, original post-swap
    let partitionCount = 0;
    if (intermediateExists) {
      const partitions = await this.start((conn) =>
        intermediate.partitions(conn),
      );
      partitionCount = partitions.length;
    } else {
      const partitions = await this.start((conn) => table.partitions(conn));
      partitionCount = partitions.length;
    }

    const mirrorTriggerExists = await table.triggerExists(
      this.pool,
      Mirroring.triggerNameFor(table, "intermediate"),
    );

    const retiredMirrorTriggerExists = await table.triggerExists(
      this.pool,
      Mirroring.triggerNameFor(table, "retired"),
    );

    const originalIsPartitioned = await table.isPartitioned(this.pool);

    return {
      intermediateExists,
      partitionCount,
      mirrorTriggerExists,
      retiredMirrorTriggerExists,
      originalIsPartitioned,
    };
  }
}
