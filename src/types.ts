/**
 * Valid partition periods for partitioned tables.
 */
export const PERIODS = ["day", "month", "year"] as const;

/**
 * A time period to partition a table by.
 */
export type Period = (typeof PERIODS)[number];

/**
 * Valid cast types for partition columns.
 */
export type Cast = "date" | "timestamptz";

/**
 * SQL formats used for partition naming by period.
 */
export const SQL_FORMAT = {
  day: "YYYYMMDD",
  month: "YYYYMM",
  year: "YYYY",
} as const;

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
 * Settings stored in a partitioned table's comment.
 */
export interface TableSettings {
  column: string;
  period: Period;
  cast: Cast;
}

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
