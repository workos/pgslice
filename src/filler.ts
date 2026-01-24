import { CommonQueryMethods, sql } from "slonik";
import { z } from "zod";
import type { Table } from "./table.js";
import type { IdComparator } from "./id-comparator.js";
import type { Cast, FillBatchResult, IdValue, TimeFilter } from "./types.js";

export interface FillerOptions {
  source: Table;
  dest: Table;
  comparator: IdComparator;
  batchSize: number;
  startingId: IdValue;
  maxSourceId: IdValue;
  includeStart: boolean;
  columns: string[];
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
  readonly #comparator: IdComparator;
  readonly #batchSize: number;
  readonly #maxSourceId: IdValue;
  readonly #columns: string[];
  readonly #timeFilter?: TimeFilter;

  #currentId: IdValue;
  #includeStart: boolean;
  #batchNumber: number;
  #totalBatches: number | null;

  constructor(options: FillerOptions) {
    this.#source = options.source;
    this.#dest = options.dest;
    this.#comparator = options.comparator;
    this.#batchSize = options.batchSize;
    this.#currentId = options.startingId;
    this.#maxSourceId = options.maxSourceId;
    this.#includeStart = options.includeStart;
    this.#columns = options.columns;
    this.#timeFilter = options.timeFilter;
    this.#batchNumber = 0;
    this.#totalBatches = this.#comparator.batchCount(
      options.startingId,
      options.maxSourceId,
      options.batchSize,
    );
  }

  /**
   * Fills data in batches, yielding results after each batch.
   * Each batch runs in its own transaction.
   *
   * @param connection - Database connection pool or query methods
   */
  async *fill(
    connection: CommonQueryMethods,
  ): AsyncGenerator<FillBatchResult> {
    while (this.#comparator.shouldContinue(this.#currentId, this.#maxSourceId)) {
      this.#batchNumber++;

      const result = await this.#processBatch(connection);
      yield result;

      this.#includeStart = false;
    }
  }

  async #processBatch(connection: CommonQueryMethods): Promise<FillBatchResult> {
    const startId = this.#currentId;

    // Build the WHERE clause
    const batchWhere = this.#comparator.batchWhereCondition(
      this.#currentId,
      this.#batchSize,
      this.#includeStart,
    );

    let whereClause = batchWhere;
    if (this.#timeFilter) {
      const timeWhere = this.#buildTimeFilterClause(this.#timeFilter);
      whereClause = sql.fragment`${batchWhere} AND ${timeWhere}`;
    }

    // Build column list
    const columnList = sql.join(
      this.#columns.map((col) => sql.identifier([col])),
      sql.fragment`, `,
    );

    // Build the SELECT suffix (ORDER BY/LIMIT for ULIDs)
    const selectSuffix = this.#comparator.selectSuffix(this.#batchSize);

    // Build and execute the INSERT query
    const insertQuery = selectSuffix
      ? sql.type(z.object({}))`
          INSERT INTO ${this.#dest.toSqlIdentifier()} (${columnList})
          SELECT ${columnList}
          FROM ${this.#source.toSqlIdentifier()}
          WHERE ${whereClause}
          ${selectSuffix}
          ON CONFLICT DO NOTHING
        `
      : sql.type(z.object({}))`
          INSERT INTO ${this.#dest.toSqlIdentifier()} (${columnList})
          SELECT ${columnList}
          FROM ${this.#source.toSqlIdentifier()}
          WHERE ${whereClause}
          ON CONFLICT DO NOTHING
        `;

    const queryResult = await connection.query(insertQuery);

    // Get the next starting ID
    this.#currentId = await this.#comparator.nextStartingId(
      this.#currentId,
      this.#batchSize,
      connection,
      this.#source,
    );

    return {
      batchNumber: this.#batchNumber,
      totalBatches: this.#totalBatches,
      rowsInserted: queryResult.rowCount,
      startId,
      endId: this.#currentId,
    };
  }

  #buildTimeFilterClause(timeFilter: TimeFilter) {
    const timeCol = sql.identifier([timeFilter.column]);
    const startDate = this.#formatDateForSql(
      timeFilter.startingTime,
      timeFilter.cast,
    );
    const endDate = this.#formatDateForSql(
      timeFilter.endingTime,
      timeFilter.cast,
    );

    return sql.fragment`${timeCol} >= ${startDate} AND ${timeCol} < ${endDate}`;
  }

  #formatDateForSql(date: Date, cast: Cast) {
    const year = date.getUTCFullYear();
    const month = String(date.getUTCMonth() + 1).padStart(2, "0");
    const day = String(date.getUTCDate()).padStart(2, "0");

    if (cast === "timestamptz") {
      return sql.literalValue(`${year}-${month}-${day} 00:00:00 UTC`);
    }
    return sql.literalValue(`${year}-${month}-${day}`);
  }
}
