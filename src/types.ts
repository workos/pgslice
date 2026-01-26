/**
 * Valid partition periods for partitioned tables.
 */
export const PERIODS = ["day", "month", "year"] as const;

/**
 * Type guard to check if a string is a valid Period.
 */
export function isPeriod(value: string): value is Period {
  return PERIODS.includes(value as Period);
}

/**
 * A time period to partition a table by.
 */
export type Period = (typeof PERIODS)[number];

/**
 * Valid cast types for partition columns.
 */
export type Cast = "date" | "timestamptz";

/**
 * Column metadata from the database.
 */
export interface ColumnInfo {
  name: string;
  dataType: string;
  cast: Cast | null;
}

/**
 * SQL formats used for partition naming by period.
 */
export const SQL_FORMAT = {
  day: "YYYYMMDD",
  month: "YYYYMM",
  year: "YYYY",
} as const satisfies Record<Period, string>;

/**
 * Options for the `prep` command.
 */
export type PrepOptions = {
  table: string;
} & (
  | {
      partition: false;
    }
  | {
      partition?: true;
      column: string;
      period: Period;
    }
);

/**
 * Options for the `add_partitions` command.
 */
export interface AddPartitionsOptions {
  table: string;
  intermediate?: boolean;
  past?: number;
  future?: number;
  tablespace?: string;
}

/**
 * Options for the `enable_mirroring` command.
 */
export interface EnableMirroringOptions {
  table: string;
}

/**
 * Options for the `disable_mirroring` command.
 */
export interface DisableMirroringOptions {
  table: string;
}

/**
 * Represents a primary key value that can be either numeric (bigint) or string (ULID).
 */
export type IdValue = bigint | string;

/**
 * Time filter configuration for partitioned tables during fill.
 */
export interface TimeFilter {
  column: string;
  cast: Cast;
  startingTime: Date;
  endingTime: Date;
}

/**
 * Options for the `fill` command.
 */
export interface FillOptions {
  table: string;
  swapped?: boolean;
  batchSize?: number;
  start?: string;
}

/**
 * Result of a single fill batch operation.
 */
export interface FillBatchResult {
  batchNumber: number;
  rowsInserted: number;
  startId: IdValue | null;
  endId: IdValue | null;
}

/**
 * Options for the `synchronize` command.
 */
export interface SynchronizeOptions {
  table: string;
  primaryKey?: string;
  start?: string;
  windowSize?: number;
  dryRun?: boolean;
}

/**
 * Result of a single synchronize batch operation.
 */
export interface SynchronizeBatchResult {
  batchNumber: number;
  batchDurationMs: number;
  primaryKeyRange: { start: IdValue; end: IdValue };
  rowsCompared: number;
  matchingRows: number;
  rowsInserted: number;
  rowsUpdated: number;
  rowsDeleted: number;
}

/**
 * Information about a sequence attached to a table column.
 */
export interface SequenceInfo {
  sequenceSchema: string;
  sequenceName: string;
  relatedColumn: string;
}

/**
 * Options for the `swap` command.
 */
export interface SwapOptions {
  table: string;
  lockTimeout?: string;
}
