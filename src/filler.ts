import { CommonQueryMethods, sql } from "slonik";
import { z } from "zod";
import { Table } from "./table.js";
import { advanceDate, parsePartitionDate } from "./date-ranges.js";
import type { FillBatchResult, FillOptions, IdValue, TimeFilter } from "./types.js";
import { formatDateForSql } from "./sql-utils.js";

/**
 * Zod schema for validating ID values from the database.
 */
const idValueSchema = z.union([z.bigint(), z.number(), z.string()]).nullable();

/**
 * Transforms a raw ID value from the database into the proper IdValue type.
 * Numbers and numeric strings become bigint, ULID strings stay as strings.
 */
function transformIdValue(
  val: bigint | number | string | null,
): IdValue | null {
  if (val === null) {
    return null;
  }
  if (typeof val === "bigint") {
    return val;
  }
  if (typeof val === "number") {
    return BigInt(val);
  }
  // val is string - check if it's a numeric string or ULID
  if (/^\d+$/.test(val)) {
    return BigInt(val);
  }
  return val;
}

export interface FillerOptions {
  source: Table;
  dest: Table;
  primaryKeyColumn: string;
  batchSize: number;
  columns: string[];
  startingId?: IdValue;
  includeStart?: boolean;
  timeFilter?: TimeFilter;
}

/**
 * Handles batch filling of data from a source table to a destination table.
 * Each batch is processed in its own transaction to ensure progress is committed
 * independently and can be resumed if interrupted.
 */
export class Filler {
  readonly #source: Table;
  readonly #dest: Table;
  readonly #primaryKeyColumn: string;
  readonly #batchSize: number;
  readonly #columns: string[];
  readonly #timeFilter?: TimeFilter;
  readonly #includeStart: boolean;
  readonly #startingId: IdValue | null;

  private constructor(options: FillerOptions) {
    this.#source = options.source;
    this.#dest = options.dest;
    this.#primaryKeyColumn = options.primaryKeyColumn;
    this.#batchSize = options.batchSize;
    this.#columns = options.columns;
    this.#timeFilter = options.timeFilter;
    this.#includeStart = options.includeStart ?? false;
    this.#startingId = options.startingId ?? null;
  }

  /**
   * Factory method to create a Filler from FillOptions.
   * Resolves source/dest tables, fetches partition settings, and determines
   * the starting position for filling.
   */
  static async init(
    tx: CommonQueryMethods,
    options: FillOptions,
  ): Promise<Filler> {
    const table = Table.parse(options.table);

    // Resolve source and dest tables based on swapped option
    const sourceTable = options.swapped ? table.retired() : table;
    const destTable = options.swapped ? table : table.intermediate();

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

    // Determine starting ID and includeStart flag
    let startingId: IdValue | undefined;
    let includeStart = false;

    if (options.start !== undefined) {
      // Use the provided start value (inclusive)
      includeStart = true;
      // Parse as bigint if numeric, otherwise keep as string (ULID)
      startingId = /^\d+$/.test(options.start)
        ? BigInt(options.start)
        : options.start;
    } else if (options.swapped) {
      // Get max from dest - resume from where we left off (exclusive)
      const maxSourceId = await sourceTable.maxId(tx, primaryKeyColumn);
      const destMaxId = await destTable.maxId(tx, primaryKeyColumn, {
        below: maxSourceId ?? undefined,
      });
      startingId = destMaxId ?? undefined;
    } else {
      // Get max from dest - resume from where we left off (exclusive)
      const destMaxId = await destTable.maxId(tx, primaryKeyColumn);
      startingId = destMaxId ?? undefined;
    }

    return new Filler({
      source: sourceTable,
      dest: destTable,
      primaryKeyColumn,
      batchSize: options.batchSize ?? 10_000,
      columns,
      startingId,
      includeStart,
      timeFilter,
    });
  }

  /**
   * Fills data in batches, yielding results after each batch.
   * Each batch runs in its own transaction.
   *
   * @param connection - Database connection pool or query methods
   */
  async *fill(connection: CommonQueryMethods): AsyncGenerator<FillBatchResult> {
    let currentId = this.#startingId;
    let includeStart = this.#includeStart;
    let batchNumber = 0;

    while (true) {
      batchNumber++;

      const result = await this.#processBatch(
        connection,
        currentId,
        includeStart,
      );

      // Update current ID for next batch
      if (result.endId !== null) {
        currentId = result.endId;
      }

      // Stop when no rows were inserted (source exhausted)
      if (result.rowsInserted === 0) {
        break;
      }

      yield { ...result, batchNumber };

      // After first batch, always use exclusive comparison
      includeStart = false;
    }
  }

  async #processBatch(
    connection: CommonQueryMethods,
    currentId: IdValue | null,
    includeStart: boolean,
  ): Promise<{
    rowsInserted: number;
    startId: IdValue | null;
    endId: IdValue | null;
  }> {
    const startId = currentId;
    const pkCol = sql.identifier([this.#primaryKeyColumn]);

    // Build WHERE conditions
    const conditions = [];

    // Add primary key condition if we have a starting ID
    if (currentId !== null) {
      if (includeStart) {
        conditions.push(sql.fragment`${pkCol} >= ${currentId}`);
      } else {
        conditions.push(sql.fragment`${pkCol} > ${currentId}`);
      }
    }

    // Add time filter conditions if present
    if (this.#timeFilter) {
      const timeCol = sql.identifier([this.#timeFilter.column]);
      const startDate = formatDateForSql(
        this.#timeFilter.startingTime,
        this.#timeFilter.cast,
      );
      const endDate = formatDateForSql(
        this.#timeFilter.endingTime,
        this.#timeFilter.cast,
      );
      conditions.push(
        sql.fragment`${timeCol} >= ${startDate} AND ${timeCol} < ${endDate}`,
      );
    }

    // Build the final WHERE clause
    const whereClause =
      conditions.length > 0
        ? sql.join(conditions, sql.fragment` AND `)
        : sql.fragment`TRUE`;

    // Build column list
    const columnList = sql.join(
      this.#columns.map((col) => sql.identifier([col])),
      sql.fragment`, `,
    );

    // Build and execute the CTE-based INSERT query
    const result = await connection.one(
      sql.type(
        z.object({
          max_id: idValueSchema,
          count: z.coerce.number(),
        }),
      )`
        WITH batch AS (
          INSERT INTO ${this.#dest.toSqlIdentifier()} (${columnList})
          SELECT ${columnList}
          FROM ${this.#source.toSqlIdentifier()}
          WHERE ${whereClause}
          ORDER BY ${pkCol}
          LIMIT ${this.#batchSize}
          ON CONFLICT DO NOTHING
          RETURNING ${pkCol}
        )
        SELECT MAX(${pkCol}) AS max_id, COUNT(*)::int AS count FROM batch
      `,
    );

    const endId = transformIdValue(result.max_id);

    return {
      rowsInserted: result.count,
      startId,
      endId,
    };
  }
}
