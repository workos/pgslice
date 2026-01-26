import {
  CommonQueryMethods,
  createPool,
  DatabaseTransactionConnection,
  sql,
  type DatabasePool,
} from "slonik";
import { z } from "zod";

import type {
  AddPartitionsOptions,
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
  UnswapOptions,
} from "./types.js";
import { isPeriod } from "./types.js";
import { Table, getServerVersionNum } from "./table.js";
import { DateRanges } from "./date-ranges.js";
import { formatDateForSql, rawSql } from "./sql-utils.js";
import { Mirroring } from "./mirroring.js";
import { Filler } from "./filler.js";
import { Synchronizer } from "./synchronizer.js";
import { Swapper } from "./swapper.js";

interface PgsliceOptions {
  dryRun?: boolean;
}

export class Pgslice {
  #connection: DatabasePool | CommonQueryMethods | null = null;
  #dryRun: boolean;

  constructor(
    connection: DatabasePool | CommonQueryMethods,
    options: PgsliceOptions,
  ) {
    this.#dryRun = options.dryRun ?? false;
    this.#connection = connection;
  }

  static async connect(
    databaseUrl: URL,
    options: PgsliceOptions = {},
  ): Promise<Pgslice> {
    const connection = await createPool(databaseUrl.toString());
    const instance = new Pgslice(connection, options);
    return instance;
  }

  private get connection() {
    if (!this.#connection) {
      throw new Error("Not connected to the database");
    }

    return this.#connection;
  }

  async start<T>(
    handler: (transaction: DatabaseTransactionConnection) => Promise<T>,
  ): Promise<T> {
    if (this.#dryRun) {
      throw new Error("Dry run not yet supported.");
    }

    return this.connection.transaction(handler);
  }

  async close(): Promise<void> {
    if (this.#connection) {
      if ("end" in this.#connection) {
        await this.#connection.end();
      }
      this.#connection = null;
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
    tx: DatabaseTransactionConnection,
    options: PrepOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);
    const intermediate = table.intermediate();

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
      await this.#createUnpartitionedIntermediateTable(tx, table, intermediate);
    }
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
    const intermediateIdent = intermediate.toSqlIdentifier();
    const tableIdent = table.toSqlIdentifier();
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
      sql.type(z.object({}))`
        CREATE TABLE ${intermediateIdent} (LIKE ${tableIdent}
          INCLUDING ${sql.join(includings, sql.fragment` INCLUDING `)}
        ) PARTITION BY RANGE (${columnIdent})
      `,
    );

    // Copy indexes
    for (const indexDef of await table.indexDefs(tx)) {
      // Transform the index definition to point to the intermediate table
      const transformedIndexDef = indexDef
        .replace(
          / ON \S+ USING /,
          ` ON ${intermediate.toQuotedString()} USING `,
        )
        .replace(/ INDEX .+ ON /, " INDEX ON ");
      await tx.query(sql.type(z.object({}))`${rawSql(transformedIndexDef)}`);
    }

    // Copy foreign keys
    await this.#copyForeignKeys(tx, table, intermediate);

    // Add metadata comment - use cast from columnInfo, default to 'date' if not a timestamp type
    const cast = columnInfo.cast ?? "date";
    const comment = `column:${columnInfo.name},period:${period},cast:${cast},version:3`;
    await tx.query(
      sql.type(z.object({}))`
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
      sql.type(z.object({}))`
        CREATE TABLE ${intermediate.toSqlIdentifier()} (LIKE ${table.toSqlIdentifier()} INCLUDING ALL)
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
        sql.type(
          z.object({}),
        )`ALTER TABLE ${target.toSqlIdentifier()} ADD ${rawSql(fkDef)}`,
      );
    }
  }

  /**
   * Adds partitions to a partitioned table.
   */
  async addPartitions(
    tx: DatabaseTransactionConnection,
    options: AddPartitionsOptions,
  ): Promise<void> {
    const originalTable = Table.parse(options.table);
    const targetTable = options.intermediate
      ? originalTable.intermediate()
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

    const primaryKeyColumns = await schemaTable.primaryKey(tx);

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
        CREATE TABLE ${partitionTable.toSqlIdentifier()}
        PARTITION OF ${targetTable.toSqlIdentifier()}
        FOR VALUES FROM (${startDate}) TO (${endDate})
      `;

      if (options.tablespace) {
        createSql = sql.fragment`${createSql} TABLESPACE ${sql.identifier([options.tablespace])}`;
      }

      await tx.query(sql.type(z.object({}))`${createSql}`);

      // Add primary key if the schema table has one
      if (primaryKeyColumns.length > 0) {
        const pkColumns = sql.join(
          primaryKeyColumns.map((col) => sql.identifier([col])),
          sql.fragment`, `,
        );
        await tx.query(
          sql.type(z.object({}))`
            ALTER TABLE ${partitionTable.toSqlIdentifier()}
            ADD PRIMARY KEY (${pkColumns})
          `,
        );
      }
    }
  }

  /**
   * Enables mirroring triggers from a table to its intermediate table.
   * This ensures that INSERT, UPDATE, and DELETE operations on the source
   * table are automatically replicated to the intermediate table.
   */
  async enableMirroring(
    tx: DatabaseTransactionConnection,
    options: EnableMirroringOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);
    const intermediate = table.intermediate();

    if (!(await table.exists(tx))) {
      throw new Error(`Table not found: ${table.toString()}`);
    }
    if (!(await intermediate.exists(tx))) {
      throw new Error(`Table not found: ${intermediate.toString()}`);
    }

    await new Mirroring({
      source: table,
      target: intermediate,
      mode: "intermediate",
    }).enable(tx);
  }

  /**
   * Disables mirroring triggers from a table to its intermediate table.
   * This removes the triggers that were created by enableMirroring.
   */
  async disableMirroring(
    tx: DatabaseTransactionConnection,
    options: DisableMirroringOptions,
  ): Promise<void> {
    const table = Table.parse(options.table);
    const intermediate = table.intermediate();

    if (!(await table.exists(tx))) {
      throw new Error(`Table not found: ${table.toString()}`);
    }

    await new Mirroring({
      source: table,
      target: intermediate,
      mode: "intermediate",
    }).disable(tx);
  }

  /**
   * Fills the destination table from the source table in batches.
   * Each batch runs in its own transaction to allow resumable progress.
   *
   * @param options - Fill options including table names and batch configuration
   * @yields FillBatchResult after each batch is processed
   */
  async *fill(options: FillOptions): AsyncGenerator<FillBatchResult> {
    const filler = await this.start((tx) => Filler.init(tx, options));

    for await (const batch of filler.fill(this.connection)) {
      yield batch;
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
    options: SynchronizeOptions,
  ): AsyncGenerator<SynchronizeBatchResult> {
    const synchronizer = await this.start((tx) =>
      Synchronizer.init(tx, options),
    );

    for await (const batch of synchronizer.synchronize(this.connection)) {
      yield batch;
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
    tx: DatabaseTransactionConnection,
    options: SwapOptions,
  ): Promise<void> {
    const swapper = new Swapper({
      table: options.table,
      direction: "forward",
      lockTimeout: options.lockTimeout,
    });
    await swapper.execute(tx);
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
    tx: DatabaseTransactionConnection,
    options: UnswapOptions,
  ): Promise<void> {
    const swapper = new Swapper({
      table: options.table,
      direction: "reverse",
      lockTimeout: options.lockTimeout,
    });
    await swapper.execute(tx);
  }
}
