import {
  CommonQueryMethods,
  createPool,
  DatabaseTransactionConnection,
  SlonikError,
  sql,
  type DatabasePool,
} from "slonik";
import { z } from "zod";

import type {
  AddPartitionsOptions,
  DisableMirroringOptions,
  EnableMirroringOptions,
  FillBatchResult,
  FillOptions,
  IdValue,
  Period,
  PrepOptions,
  TimeFilter,
} from "./types.js";
import { isPeriod } from "./types.js";
import { Table, getServerVersionNum } from "./table.js";
import { DateRanges, advanceDate, parsePartitionDate } from "./date-ranges.js";
import { formatDateForSql, rawSql } from "./sql-utils.js";
import { Mirroring } from "./mirroring.js";
import { Filler } from "./filler.js";
import { isUlid } from "./id-comparator.js";

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
      if (!columns.includes(options.column)) {
        throw new Error(`Column not found: ${options.column}`);
      }

      if (!isPeriod(options.period)) {
        throw new Error(`Invalid period: ${options.period}`);
      }

      await this.#createPartitionedIntermediateTable(
        tx,
        table,
        intermediate,
        options.column,
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
    column: string,
    period: Period,
  ): Promise<void> {
    const serverVersionNum = await getServerVersionNum(tx);

    // Create partitioned table using the appropriate INCLUDING clauses
    const intermediateIdent = intermediate.toSqlIdentifier();
    const tableIdent = table.toSqlIdentifier();
    const columnIdent = sql.identifier([column]);

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

    // Add metadata comment
    const cast = await table.columnCast(tx, column);
    const comment = `column:${column},period:${period},cast:${cast},version:3`;
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

    await new Mirroring({ source: table, target: intermediate }).enable(tx);
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

    await new Mirroring({ source: table, target: intermediate }).disable(tx);
  }

  /**
   * Fills the destination table from the source table in batches.
   * Each batch runs in its own transaction to allow resumable progress.
   *
   * @param options - Fill options including table names and batch configuration
   * @yields FillBatchResult after each batch is processed
   */
  async *fill(options: FillOptions): AsyncGenerator<FillBatchResult> {
    const table = Table.parse(options.table);

    // Resolve source and dest tables based on options
    let sourceTable: Table;
    let destTable: Table;

    if (options.sourceTable) {
      sourceTable = Table.parse(options.sourceTable);
    } else if (options.swapped) {
      sourceTable = table.retired();
    } else {
      sourceTable = table;
    }

    if (options.destTable) {
      destTable = Table.parse(options.destTable);
    } else if (options.swapped) {
      destTable = table;
    } else {
      destTable = table.intermediate();
    }

    // Use a transaction for the initial setup/metadata reads
    const setupResult = await this.start(async (tx) => {
      // Verify tables exist
      if (!(await sourceTable.exists(tx))) {
        throw new Error(`Table not found: ${sourceTable.toString()}`);
      }
      if (!(await destTable.exists(tx))) {
        throw new Error(`Table not found: ${destTable.toString()}`);
      }

      // Get partition settings from dest table for time filtering
      const settings = await destTable.fetchSettings(tx);

      // Determine time filter if dest is partitioned
      let timeFilter: TimeFilter | undefined;
      if (settings) {
        const partitions = await destTable.partitions(tx);
        if (partitions.length > 0) {
          const firstPartition = partitions[0];
          const lastPartition = partitions[partitions.length - 1];

          const startingTime = parsePartitionDate(
            firstPartition.name,
            settings.period,
          );
          const lastPartitionDate = parsePartitionDate(
            lastPartition.name,
            settings.period,
          );
          const endingTime = advanceDate(lastPartitionDate, settings.period, 1);

          timeFilter = {
            column: settings.column,
            cast: settings.cast,
            startingTime,
            endingTime,
          };
        }
      }

      // Determine which table to get the schema (columns, primary key) from
      let schemaTable: Table;
      if (settings) {
        const partitions = await destTable.partitions(tx);
        schemaTable =
          partitions.length > 0 ? partitions[partitions.length - 1] : table;
      } else {
        schemaTable = table;
      }

      // Get primary key
      const primaryKeyColumns = await schemaTable.primaryKey(tx);
      if (primaryKeyColumns.length === 0) {
        throw new Error("No primary key");
      }
      const primaryKeyColumn = primaryKeyColumns[0];

      // Get columns from source table
      const columns = await sourceTable.columns(tx);

      // Get max source ID
      let maxSourceId: IdValue | null;
      try {
        maxSourceId = await sourceTable.maxId(tx, primaryKeyColumn);
      } catch (error) {
        if (
          error instanceof SlonikError &&
          error.message.includes("UndefinedFunction")
        ) {
          throw new Error("Only numeric and ULID primary keys are supported");
        }
        throw error;
      }

      // Determine starting ID
      let startingId: IdValue | null;
      let includeStart = false;

      if (options.start !== undefined) {
        // Use the provided start value
        includeStart = true;
        if (isUlid(options.start)) {
          startingId = options.start;
        } else {
          startingId = BigInt(options.start);
        }
      } else if (options.swapped) {
        // Get max from dest where id <= maxSourceId
        startingId = await destTable.maxId(tx, primaryKeyColumn, {
          below: maxSourceId ?? undefined,
        });
      } else {
        // Get max from dest
        startingId = await destTable.maxId(tx, primaryKeyColumn);
      }

      // Handle case where dest is empty and not swapped
      const comparator = await sourceTable.createIdComparator(
        tx,
        primaryKeyColumn,
        options.start,
      );

      if (
        (startingId === null ||
          startingId === comparator.minValue ||
          startingId === 0n) &&
        !options.swapped
      ) {
        const minSourceId = await sourceTable.minId(tx, primaryKeyColumn, {
          column: timeFilter?.column,
          cast: timeFilter?.cast,
          startingTime: timeFilter?.startingTime,
        });

        if (minSourceId !== null) {
          startingId = comparator.predecessor(minSourceId);
        }
      }

      // If still no max source ID and no start option, nothing to fill
      if (maxSourceId === null && options.start === undefined) {
        return null;
      }

      // At this point, either maxSourceId is not null, or options.start was provided
      // (which means startingId is also not null).
      const finalMaxSourceId = maxSourceId ?? startingId;
      if (finalMaxSourceId === null) {
        // This should never happen based on the logic above, but TypeScript can't know that
        throw new Error(
          "Unexpected: maxSourceId should be defined at this point",
        );
      }

      const finalStartingId = startingId ?? comparator.minValue;

      return {
        sourceTable,
        destTable,
        primaryKeyColumn,
        columns,
        maxSourceId: finalMaxSourceId,
        startingId: finalStartingId,
        comparator,
        timeFilter,
        includeStart,
      };
    });

    // Nothing to fill
    if (setupResult === null) {
      return;
    }

    const batchSize = options.batchSize ?? 10000;
    const batchCount = setupResult.comparator.batchCount(
      setupResult.startingId,
      setupResult.maxSourceId,
      batchSize,
    );

    // If numeric and batch count is 0, nothing to fill
    if (batchCount === 0) {
      return;
    }

    // Create the filler
    const filler = new Filler({
      source: setupResult.sourceTable,
      dest: setupResult.destTable,
      comparator: setupResult.comparator,
      batchSize,
      startingId: setupResult.startingId,
      maxSourceId: setupResult.maxSourceId,
      includeStart: setupResult.includeStart,
      columns: setupResult.columns,
      timeFilter: setupResult.timeFilter,
    });

    // Process batches - each batch in its own transaction
    for await (const batch of filler.fill(this.connection)) {
      yield batch;
    }
  }
}

