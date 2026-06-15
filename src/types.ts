/**
 * Valid partition periods for partitioned tables.
 */
export const PERIODS = ["day", "week", "month", "year"] as const;

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
  week: 'IYYY"w"IW',
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
  /**
   * When true (the default), each newly created partition is granted the same
   * privileges that are present on the partitioned parent table. Postgres does
   * not propagate parent grants to partitions automatically, so without this a
   * role such as a CDC/replication user loses access to new
   * partitions until a grant is run manually.
   */
  inheritGrants?: boolean;
}

/**
 * Options for the `maintain` command.
 */
export interface MaintainOptions {
  past?: number;
  future?: number;
  /**
   * Restrict maintenance to partitioned tables in this schema. When omitted,
   * every managed partitioned table the connection can see is maintained.
   */
  schema?: string;
  tablespace?: string;
  inheritGrants?: boolean;
}

/**
 * How a managed table's primary key is owned, which decides how `add_partitions`
 * treats each new partition.
 * - "native": the partitioned parent owns the (often composite) primary key, so
 *   Postgres propagates it — and any partitioned indexes — to each new partition.
 * - "pgslice": the classic pgslice model where the parent has no primary key and
 *   each partition owns its own.
 */
export type PartitionModel = "native" | "pgslice";

/**
 * Outcome of maintaining a single managed table.
 */
export interface MaintainResult {
  table: string;
  /** The partitioning model, or null if maintenance failed before it was determined. */
  model: PartitionModel | null;
  partitionsCreated: string[];
  partitionCount: number;
  /**
   * Whether every leaf partition has a replica identity usable for logical
   * replication. A new partition is created with the default replica identity
   * (`relreplident='d'`), so its row identity is its own (or inherited) primary
   * key — Postgres does not copy a parent's `REPLICA IDENTITY USING INDEX`
   * choice down to leaves, and does not need to: the parent's partitioned
   * replica-identity index still propagates and stays valid, but it governs the
   * parent's identity, not the leaf's. New partitions are therefore CDC-safe
   * with no replica-identity DDL, provided each leaf's primary key columns are
   * the key CDC needs (for the managed tables, the `(id, key)` composite key,
   * which equals any USING INDEX identity the parent carries). This flag is a
   * read-only guard against a leaf with no usable identity, not work pgslice
   * performs.
   */
  replicaIdentityReady: boolean;
  /** Leaf partitions that would not be CDC-safe (no usable replica identity). */
  unsafePartitions: string[];
  /**
   * Error message if maintaining this table failed; null on success. One
   * table's failure (e.g. a non-empty DEFAULT blocking the next partition) is
   * recorded here and does not stop the rest of the fleet from being extended.
   */
  error: string | null;
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
