import type { Period } from "./types.js";

const MS_PER_DAY = 86_400_000;

/**
 * Returns the ISO weekday (Mon=1 .. Sun=7) for a date in UTC.
 */
function isoWeekday(date: Date): number {
  const day = date.getUTCDay();
  return day === 0 ? 7 : day;
}

/**
 * Returns the UTC Monday that starts the ISO week containing the given date.
 */
function startOfIsoWeek(date: Date): Date {
  const monday = new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
  monday.setUTCDate(monday.getUTCDate() - (isoWeekday(date) - 1));
  return monday;
}

/**
 * Computes the ISO week-numbering year and week number for a date.
 * Per ISO 8601, week 1 is the week containing the year's first Thursday.
 */
function isoWeekInfo(date: Date): { isoYear: number; isoWeek: number } {
  const thursday = startOfIsoWeek(date);
  thursday.setUTCDate(thursday.getUTCDate() + 3);

  const isoYear = thursday.getUTCFullYear();
  const firstThursday = new Date(Date.UTC(isoYear, 0, 4));
  firstThursday.setUTCDate(
    firstThursday.getUTCDate() - (isoWeekday(firstThursday) - 1) + 3,
  );

  const isoWeek =
    1 +
    Math.round(
      (thursday.getTime() - firstThursday.getTime()) / (7 * MS_PER_DAY),
    );

  return { isoYear, isoWeek };
}

/**
 * Returns the UTC Monday that starts the given ISO week-numbering year/week.
 */
function isoWeekToMonday(isoYear: number, isoWeek: number): Date {
  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const week1Monday = new Date(jan4);
  week1Monday.setUTCDate(jan4.getUTCDate() - (isoWeekday(jan4) - 1));
  week1Monday.setUTCDate(week1Monday.getUTCDate() + (isoWeek - 1) * 7);
  return week1Monday;
}

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
    case "week":
      return startOfIsoWeek(date);
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
    case "week":
      return new Date(Date.UTC(year, month, day + count * 7));
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
    case "week": {
      const { isoYear, isoWeek } = isoWeekInfo(date);
      return `${isoYear}w${isoWeek.toString().padStart(2, "0")}`;
    }
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
    case "week": {
      // Format: <ISO year>w<ISO week>, e.g. "2026w32". A legacy-prefixed suffix
      // (e.g. "y2026w03") would split to NaN and yield an Invalid Date; fail
      // loudly instead so misuse on a legacy-named table is immediately visible.
      if (!/^\d{4}w\d{2}$/.test(suffix)) {
        throw new Error(
          `Unrecognized week partition suffix "${suffix}" in "${partitionName}"`,
        );
      }
      const [isoYear, isoWeek] = suffix.split("w");
      return isoWeekToMonday(parseInt(isoYear, 10), parseInt(isoWeek, 10));
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

/**
 * The bounds of a single existing range partition, read from the catalog.
 *
 * A `null` lower means the partition is unbounded below (`MINVALUE`); a `null`
 * upper means unbounded above (`MAXVALUE`). The DEFAULT partition has no bounds.
 */
export interface ExistingRange {
  lower: Date | null;
  upper: Date | null;
  lowerUnbounded: boolean;
  upperUnbounded: boolean;
  isDefault: boolean;
  /** The partition's catalog name, when read from the catalog. */
  name?: string;
}

/** Backstop so a malformed anchor/horizon can never spin forever. */
const MAX_GENERATED_PARTITIONS = 100_000;

/**
 * Parses a single RANGE bound value (the text inside `FROM (...)` / `TO (...)`)
 * into a UTC instant, or `null` for a value we can't read as temporal (e.g. an
 * integer range). `MINVALUE`/`MAXVALUE` are reported as unbounded.
 *
 * Boundaries are read under a UTC-pinned session, so a `timestamptz` renders
 * with a `+00` offset and a `date`/`timestamp` without one. We parse the date
 * and any time component as UTC (the `+00` is redundant), so a boundary that
 * isn't UTC-midnight anchors at its exact instant rather than being truncated
 * to the day.
 */
function parseBoundValue(value: string): {
  date: Date | null;
  unbounded: boolean;
} | null {
  const token = value.trim();
  if (token === "MINVALUE" || token === "MAXVALUE") {
    return { date: null, unbounded: true };
  }
  // A single-column RANGE bound is exactly one quoted literal; anything else
  // (e.g. a multi-column key like `'2026-01-01', 5`) is a shape we don't manage.
  const literal = token.match(/^'([^']*)'$/);
  if (!literal) {
    return null;
  }
  const match = literal[1].match(
    /(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?))?/,
  );
  if (!match) {
    return null;
  }
  const parsed = new Date(`${match[1]}T${match[2] ?? "00:00:00"}Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return { date: parsed, unbounded: false };
}

/**
 * Parses a partition's `pg_get_expr(relpartbound, ...)` text into an
 * {@link ExistingRange}. Returns `null` for non-RANGE bounds we don't manage.
 */
export function parseRangeBound(bound: string): ExistingRange | null {
  if (bound.trim() === "DEFAULT") {
    return {
      lower: null,
      upper: null,
      lowerUnbounded: true,
      upperUnbounded: true,
      isDefault: true,
    };
  }

  const match = bound.match(/FOR VALUES FROM \((.+)\) TO \((.+)\)/s);
  if (!match) {
    return null;
  }

  const lower = parseBoundValue(match[1]);
  const upper = parseBoundValue(match[2]);
  if (lower === null || upper === null) {
    // A RANGE bound we can't read as temporal (e.g. an integer range): treat it
    // as unmanaged rather than letting a null bound read as ±infinity in
    // rangeOverlaps, which would block every new partition for the table.
    return null;
  }
  return {
    lower: lower.date,
    upper: upper.date,
    lowerUnbounded: lower.unbounded,
    upperUnbounded: upper.unbounded,
    isDefault: false,
  };
}

/** The greatest finite upper bound among non-DEFAULT partitions, or null. */
export function maxUpperBound(ranges: readonly ExistingRange[]): Date | null {
  let max: Date | null = null;
  for (const range of ranges) {
    if (range.isDefault || range.upperUnbounded || range.upper === null) {
      continue;
    }
    if (max === null || range.upper.getTime() > max.getTime()) {
      max = range.upper;
    }
  }
  return max;
}

/** The least finite lower bound among non-DEFAULT partitions, or null. */
export function minLowerBound(ranges: readonly ExistingRange[]): Date | null {
  let min: Date | null = null;
  for (const range of ranges) {
    if (range.isDefault || range.lowerUnbounded || range.lower === null) {
      continue;
    }
    if (min === null || range.lower.getTime() < min.getTime()) {
      min = range.lower;
    }
  }
  return min;
}

/**
 * Whether a candidate `[start, end)` overlaps an existing partition's range,
 * treating `MINVALUE`/`MAXVALUE` as -/+ infinity. The DEFAULT partition never
 * overlaps (it only absorbs rows no explicit range claims).
 */
export function rangeOverlaps(
  start: Date,
  end: Date,
  range: ExistingRange,
): boolean {
  if (range.isDefault) {
    return false;
  }
  const lower =
    range.lowerUnbounded || range.lower === null
      ? -Infinity
      : range.lower.getTime();
  const upper =
    range.upperUnbounded || range.upper === null
      ? Infinity
      : range.upper.getTime();
  return start.getTime() < upper && end.getTime() > lower;
}

/**
 * Generates contiguous period-aligned ranges starting at `anchorStart` and
 * continuing while the start is at or before `horizon`.
 *
 * Unlike {@link DateRanges} (which aligns to an absolute calendar around
 * "today"), this chains each range off the previous one's end. That makes
 * extension scheme-agnostic: it continues an existing table's partitioning
 * contiguously whether the table uses ISO weeks, year-resetting weeks, or
 * calendar months — never introducing a gap or an overlap at the boundary.
 */
export function* extendRanges(options: {
  anchorStart: Date;
  period: Period;
  horizon: Date;
}): Generator<DateRange> {
  let start = options.anchorStart;
  for (
    let count = 0;
    start.getTime() <= options.horizon.getTime() &&
    count < MAX_GENERATED_PARTITIONS;
    count++
  ) {
    const end = advanceDate(start, options.period, 1);
    yield { start, end, suffix: formatDateSuffix(start, options.period) };
    start = end;
  }
}

/**
 * Generates contiguous period-aligned ranges ending at `anchorEnd` and
 * continuing backward while the start is at or after `horizon`. Mirror of
 * {@link extendRanges} for the `--past` direction.
 */
export function* extendRangesBackward(options: {
  anchorEnd: Date;
  period: Period;
  horizon: Date;
}): Generator<DateRange> {
  let end = options.anchorEnd;
  for (let count = 0; count < MAX_GENERATED_PARTITIONS; count++) {
    const start = advanceDate(end, options.period, -1);
    if (start.getTime() < options.horizon.getTime()) {
      break;
    }
    yield { start, end, suffix: formatDateSuffix(start, options.period) };
    end = start;
  }
}
