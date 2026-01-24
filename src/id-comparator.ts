import { CommonQueryMethods, sql, FragmentSqlToken } from "slonik";
import { z } from "zod";
import type { Table } from "./table.js";
import type { IdValue } from "./types.js";

export type { IdValue };

/**
 * ULID epoch start corresponding to 01/01/1970.
 * This is the minimum possible ULID value.
 */
export const DEFAULT_ULID = "00000H5A406P0C3DQMCQ5MV6WQ";

/**
 * Checks if a string value is a valid ULID.
 * ULIDs are 26 characters using Crockford's base32 alphabet.
 */
export function isUlid(value: string): boolean {
  return /^[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}$/.test(value);
}

/**
 * Abstract base class for comparing and handling primary key IDs during batch filling.
 * Implementations handle numeric IDs and ULID strings differently.
 */
export abstract class IdComparator<T> {
  constructor(protected readonly primaryKeyColumn: string) {}

  /**
   * The minimum possible value for this ID type.
   */
  abstract get minValue(): T;

  /**
   * Returns the predecessor of the given ID (ID - 1 for numeric, min ULID for strings).
   */
  abstract predecessor(id: T): T;

  /**
   * Determines if batch processing should continue given the current and max IDs.
   */
  abstract shouldContinue(currentId: T, maxId: T): boolean;

  /**
   * Calculates the total number of batches needed. Returns null if unknown (e.g., for ULIDs).
   */
  abstract batchCount(
    startingId: T,
    maxId: T,
    batchSize: number,
  ): number | null;

  /**
   * Builds the WHERE clause fragment for selecting a batch of rows.
   */
  abstract batchWhereCondition(
    startingId: T,
    batchSize: number,
    inclusive: boolean,
  ): FragmentSqlToken;

  /**
   * Calculates the next starting ID after processing a batch.
   * For numeric IDs this is simple addition; for ULIDs it requires querying the source.
   */
  abstract nextStartingId(
    currentId: T,
    batchSize: number,
    tx: CommonQueryMethods,
    sourceTable: Table,
  ): Promise<IdValue>;

  /**
   * Returns an optional SELECT suffix for ordering/limiting results.
   * Only needed for ULID comparators.
   */
  abstract selectSuffix(batchSize: number): FragmentSqlToken | null;
}

/**
 * Comparator for numeric (bigint) primary keys.
 * Uses range-based WHERE conditions and simple arithmetic for batch progression.
 */
export class NumericComparator extends IdComparator<BigInt> {
  get minValue(): bigint {
    return 1n;
  }

  predecessor(id: bigint): bigint {
    return id - 1n;
  }

  shouldContinue(currentId: bigint, maxId: bigint): boolean {
    return currentId < maxId;
  }

  batchCount(
    startingId: bigint,
    maxId: bigint,
    batchSize: number,
  ): number | null {
    const diff = maxId - startingId;
    if (diff <= 0n) {
      return 0;
    }
    return Math.ceil(Number(diff) / batchSize);
  }

  batchWhereCondition(
    startingId: bigint,
    batchSize: number,
    inclusive: boolean,
  ): FragmentSqlToken {
    const col = sql.identifier([this.primaryKeyColumn]);
    const endId = startingId + BigInt(batchSize);

    if (inclusive) {
      return sql.fragment`${col} >= ${startingId} AND ${col} <= ${endId}`;
    }
    return sql.fragment`${col} > ${startingId} AND ${col} <= ${endId}`;
  }

  async nextStartingId(
    currentId: bigint,
    batchSize: number,
    _tx: CommonQueryMethods,
    _sourceTable: Table,
  ): Promise<IdValue> {
    return currentId + BigInt(batchSize);
  }

  selectSuffix(_batchSize: number): FragmentSqlToken | null {
    return null;
  }
}

/**
 * Comparator for ULID string primary keys.
 * Uses comparison operators with ORDER BY/LIMIT for batch selection.
 */
export class UlidComparator extends IdComparator<string> {
  get minValue(): string {
    return DEFAULT_ULID;
  }

  predecessor(_id: string): string {
    return DEFAULT_ULID;
  }

  shouldContinue(currentId: string, maxId: string): boolean {
    return currentId < maxId;
  }

  batchCount(
    _startingId: string,
    _maxId: string,
    _batchSize: number,
  ): number | null {
    // Cannot calculate batch count for ULIDs
    return null;
  }

  batchWhereCondition(
    startingId: string,
    _batchSize: number,
    inclusive: boolean,
  ): FragmentSqlToken {
    const col = sql.identifier([this.primaryKeyColumn]);

    if (inclusive) {
      return sql.fragment`${col} >= ${startingId}`;
    }
    return sql.fragment`${col} > ${startingId}`;
  }

  async nextStartingId(
    currentId: string,
    batchSize: number,
    tx: CommonQueryMethods,
    sourceTable: Table,
  ): Promise<string> {
    // For ULIDs, we query for the max ID within the batch we want to process next.
    // We use a subquery to first limit to batchSize rows, then get MAX from that.
    const col = sql.identifier([this.primaryKeyColumn]);

    const result = await tx.maybeOne(
      sql.type(z.object({ max_id: z.string().nullable() }))`
        SELECT MAX(${col}) AS max_id
        FROM (
          SELECT ${col}
          FROM ${sourceTable.toSqlIdentifier()}
          WHERE ${col} > ${currentId}
          ORDER BY ${col}
          LIMIT ${batchSize}
        ) AS batch
      `,
    );

    return result?.max_id ?? currentId;
  }

  selectSuffix(batchSize: number): FragmentSqlToken | null {
    return sql.fragment`ORDER BY ${sql.identifier([this.primaryKeyColumn])} LIMIT ${batchSize}`;
  }
}
