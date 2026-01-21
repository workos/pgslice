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
export const SQL_FORMAT: Record<Period, string> = {
  day: "YYYYMMDD",
  month: "YYYYMM",
  year: "YYYY",
};

/**
 * Options for the `prep` command.
 */
export interface PrepOptions {
  table: string;
  column?: string;
  period?: Period;
  partition?: boolean;
}
