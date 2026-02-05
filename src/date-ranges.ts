import type { Period } from "./types.js";

/**
 * Represents a date range for a single partition.
 */
export interface DateRange {
  /** Start date of the partition range (inclusive) */
  start: Date;
  /** End date of the partition range (exclusive) */
  end: Date;
  /** Suffix for the partition table name (e.g., "20260121" for day, "202601" for month) */
  suffix: string;
}

export interface DateRangesOptions {
  /** The date to use as "today" for range calculations (defaults to current UTC date) */
  today?: Date;
  /** The period for partitioning */
  period: Period;
  /** Number of past partitions to include */
  past: number;
  /** Number of future partitions to include */
  future: number;
}

/**
 * Rounds a date down to the start of the given period.
 */
export function roundDate(date: Date, period: Period): Date {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth();
  const day = date.getUTCDate();

  switch (period) {
    case "day":
      return new Date(Date.UTC(year, month, day));
    case "month":
      return new Date(Date.UTC(year, month, 1));
    case "year":
      return new Date(Date.UTC(year, 0, 1));
  }
}

/**
 * Advances a date by the given number of periods.
 */
export function advanceDate(date: Date, period: Period, count: number): Date {
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth();
  const day = date.getUTCDate();

  switch (period) {
    case "day":
      return new Date(Date.UTC(year, month, day + count));
    case "month":
      return new Date(Date.UTC(year, month + count, 1));
    case "year":
      return new Date(Date.UTC(year + count, 0, 1));
  }
}

/**
 * Formats a date as a partition suffix based on the period.
 */
export function formatDateSuffix(date: Date, period: Period): string {
  const year = date.getUTCFullYear().toString();
  const month = (date.getUTCMonth() + 1).toString().padStart(2, "0");
  const day = date.getUTCDate().toString().padStart(2, "0");

  switch (period) {
    case "day":
      return `${year}${month}${day}`;
    case "month":
      return `${year}${month}`;
    case "year":
      return year;
  }
}

/**
 * Parses a partition table name to extract the date from its suffix.
 * The suffix is expected to be the last underscore-separated component.
 */
export function parsePartitionDate(
  partitionName: string,
  period: Period,
): Date {
  const suffix = partitionName.split("_").pop();
  if (!suffix) {
    throw new Error(`Invalid partition name: ${partitionName}`);
  }

  switch (period) {
    case "day": {
      // Format: YYYYMMDD
      const year = parseInt(suffix.slice(0, 4), 10);
      const month = parseInt(suffix.slice(4, 6), 10) - 1;
      const day = parseInt(suffix.slice(6, 8), 10);
      return new Date(Date.UTC(year, month, day));
    }
    case "month": {
      // Format: YYYYMM
      const year = parseInt(suffix.slice(0, 4), 10);
      const month = parseInt(suffix.slice(4, 6), 10) - 1;
      return new Date(Date.UTC(year, month, 1));
    }
    case "year": {
      // Format: YYYY
      const year = parseInt(suffix, 10);
      return new Date(Date.UTC(year, 0, 1));
    }
  }
}

/**
 * An iterable that generates date ranges for partitions.
 */
export class DateRanges implements Iterable<DateRange> {
  readonly #today: Date;
  readonly #period: Period;
  readonly #past: number;
  readonly #future: number;

  constructor(options: DateRangesOptions) {
    this.#today = options.today
      ? roundDate(options.today, options.period)
      : roundDate(new Date(), options.period);
    this.#period = options.period;
    this.#past = options.past;
    this.#future = options.future;
  }

  *[Symbol.iterator](): Generator<DateRange> {
    for (let n = -this.#past; n <= this.#future; n++) {
      const start = advanceDate(this.#today, this.#period, n);
      const end = advanceDate(start, this.#period, 1);
      const suffix = formatDateSuffix(start, this.#period);

      yield { start, end, suffix };
    }
  }
}
