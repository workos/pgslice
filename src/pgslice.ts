import {
  CommonQueryMethods,
  createPool,
  DatabasePoolConnection,
  DatabaseTransactionConnection,
  type DatabasePool,
} from "slonik";

import type {
  AddPartitionsOptions,
  AnalyzeOptions,
  ColumnInfo,
  DisableMirroringOptions,
  EnableMirroringOptions,
  FillBatchResult,
  FillOptions,
  Period,
  PrepOptions,
  SwapOptions,
  SynchronizeBatchResult,
  SynchronizeOptions,
  UnprepOptions,
  UnswapOptions,
} from "./types.js";
import { isPeriod } from "./types.js";
import { Table, getServerVersionNum } from "./table.js";
import { DateRanges } from "./date-ranges.js";
import { formatDateForSql, rawSql, sql } from "./sql-utils.js";
import { Mirroring } from "./mirroring.js";
import { Filler } from "./filler.js";
import { Synchronizer } from "./synchronizer.js";
import { Swapper } from "./swapper.js";
import { AdvisoryLock } from "./advisory-lock.js";

interface PgsliceOptions {
  dryRun?: boolean;

  /**
   * Whether to use Postgres advisory locks to prevent concurrent operations
   * on the same table for the same operation. Defaults to true.
   */
  advisoryLocks?: boolean;
}

export class Pgslice {
  #pool: DatabasePool | null = null;
  #dryRun: boolean;
  #advisoryLocks: boolean;

  constructor(pool: DatabasePool, options: PgsliceOptions) {
    this.#dryRun = options.dryRun ?? false;
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
    if (this.#dryRun) {
      throw new Error("Dry run not yet supported.");
    }

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
   */
  async addPartitions(
    connection: DatabasePoolConnection,
    options: AddPartitionsOptions,
  ): Promise<void> {
    const originalTable = Table.parse(options.table);

    return connection.transaction(async (tx) =>
      this.#withLock(tx, originalTable, "add_partitions", async () => {
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

        // Determine which table to get the primary key from.
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

        const primaryKeyColumn = await schemaTable.primaryKey(tx);

        const dateRanges = new DateRanges({
          period: settings.period,
          past,
          future,
        });

        for (const range of dateRanges) {
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

          await tx.query(
            sql.typeAlias("void")`
            ALTER TABLE ${partitionTable.sqlIdentifier}
            ADD PRIMARY KEY (${sql.identifier([primaryKeyColumn])})
          `,
          );
        }
      }),
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
      const filler = await Filler.init(connection, options);

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
      const synchronizer = await Synchronizer.init(connection, options);

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
}
