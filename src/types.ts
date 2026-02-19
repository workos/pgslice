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

import type { MirroringTargetType } from "./mirroring.js";

// Re-export for convenience
export type { MirroringTargetType };

/**
 * Options for the `enable_mirroring` command.
 */
export interface EnableMirroringOptions {
  table: string;
  targetType?: MirroringTargetType;
}

/**
 * Options for the `disable_mirroring` command.
 */
export interface DisableMirroringOptions {
  table: string;
  targetType?: MirroringTargetType;
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
  sourceCount: number;
  rowsInserted: number;
  startId: IdValue | null;
  endId: IdValue | null;
}

/**
 * Options for the `synchronize` command.
 */
export interface SynchronizeOptions {
  table: string;
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

/**
 * Direction of a swap operation.
 * - "forward": swap from original to intermediate (swap command)
 * - "reverse": swap from retired back to original (unswap command)
 */
export type SwapDirection = "forward" | "reverse";

/**
 * Options for the `unswap` command.
 */
export interface UnswapOptions {
  table: string;
  lockTimeout?: string;
}

/**
 * Options for the `analyze` command.
 */
export interface AnalyzeOptions {
  table: string;
  swapped?: boolean;
}

/**
 * Options for the `unprep` command.
 */
export interface UnprepOptions {
  table: string;
}

/**
 * Options for the `status` command.
 */
export interface StatusOptions {
  table: string;
}

/**
 * Status information about a table's partitioning state.
 */
export interface TableStatus {
  intermediateExists: boolean;
  partitionCount: number;
  mirrorTriggerExists: boolean;
  retiredMirrorTriggerExists: boolean;
  originalIsPartitioned: boolean;
}
