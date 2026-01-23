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
