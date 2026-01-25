import { CommonQueryMethods, sql } from "slonik";
import { z } from "zod";
import type { Table } from "./table.js";
import type { FillBatchResult, IdValue, TimeFilter } from "./types.js";
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

  constructor(options: FillerOptions) {
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

      const result = await this.#processBatch(connection, {
        currentId,
        includeStart,
      });

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
    {
      currentId,
      includeStart,
    }: {
      currentId: IdValue | null;
      includeStart: boolean;
    },
  ): Promise<{
    totalBatches: number | null;
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
      totalBatches: null,
      rowsInserted: result.count,
      startId,
      endId,
    };
  }
}
