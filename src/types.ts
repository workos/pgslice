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
  /**
   * The reference "now" for choosing the partition horizon. Defaults to the
   * current time. Pass a single value across a fleet so a run that straddles a
   * period boundary uses a consistent horizon for every table.
   */
  now?: Date;
  /**
   * How long each partition-creation statement may wait on the parent's lock
   * before backing off, rather than blocking writers queued behind it. Any
   * Postgres interval literal (e.g. "5s"). Defaults to "5s".
   */
  lockTimeout?: string;
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

/**
 * Options for the `maintain` command.
 */
export interface MaintainOptions {
  past?: number;
  /**
   * Future partitions to keep ahead, set per period. "N periods" is very
   * different runway for a weekly vs a monthly vs a yearly table, so the horizon
   * is per period and every table gets comparable forward coverage; each
   * discovered table uses the value for its own period. Defaults when omitted:
   * daily 90, weekly 26, monthly 6, yearly 1.
   */
  futureDaily?: number;
  futureWeekly?: number;
  futureMonthly?: number;
  futureYearly?: number;
  /**
   * Host of the endpoint being maintained, recorded on every log record's
   * `target` alongside the database name so operators can see which host was
   * extended. The command derives it from the connection URL (host only, no
   * credentials); when omitted, `target.host` is simply absent.
   */
  host?: string;
  /**
   * Restrict maintenance to partitioned tables in this schema. When omitted,
   * every managed partitioned table the connection can see is maintained.
   */
  schema?: string;
  tablespace?: string;
  inheritGrants?: boolean;
  /**
   * How long each partition-creation statement may wait on a table's lock
   * before backing off, rather than blocking writers queued behind it. Any
   * Postgres interval literal (e.g. "5s"). Defaults to "5s".
   */
  lockTimeout?: string;
  /**
   * The reference "now" for choosing each table's partition horizon. Defaults
   * to a single instant captured when the run starts, so a fleet run that
   * straddles a period boundary uses one consistent horizon for every table.
   */
  now?: Date;
  /**
   * Correlation id stamped on every log record for this run, so all records
   * from one invocation can be grouped. A fresh id is generated per run when
   * omitted.
   */
  jobId?: string;
}

/**
 * Sink for maintain's structured JSONL logs. Each call receives one log record;
 * the `maintain` command serializes it to stdout as one JSON object per line so
 * downstream log tooling can lift the keys into attributes. Only `info` and
 * `error` records are emitted — there is no debug/warning level.
 */
export type MaintainLog = (entry: Record<string, unknown>) => void;

/**
 * How a managed table's primary key is owned, which decides how
 * `add_partitions` treats each new partition.
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
   * replication. New partitions are created with the default replica identity,
   * so each leaf's row identity is its own (or inherited) primary key and no
   * replica-identity DDL is required; this read-only flag surfaces a leaf
   * lacking a usable identity rather than shipping a CDC-unsafe partition.
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
