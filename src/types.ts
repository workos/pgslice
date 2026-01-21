/**
 * Valid partition periods for partitioned tables.
 */
export type Period = "day" | "month" | "year";

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
