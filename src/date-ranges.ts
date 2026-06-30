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
  // The Thursday of the ISO week containing `date` determines the ISO year and
  // anchors the week count (ISO week 1 is the week containing the year's first
  // Thursday).
  const weekMonday = startOfIsoWeek(date);
  const thursday = new Date(weekMonday);
  thursday.setUTCDate(weekMonday.getUTCDate() + 3);

  const isoYear = thursday.getUTCFullYear();
  // The Thursday of ISO week 1: Jan 4 is always in week 1, so take the Monday
  // of its week and advance to Thursday.
  const jan4 = new Date(Date.UTC(isoYear, 0, 4));
  const week1Thursday = new Date(jan4);
  week1Thursday.setUTCDate(jan4.getUTCDate() - (isoWeekday(jan4) - 1) + 3);

  const isoWeek =
    1 +
    Math.round(
      (thursday.getTime() - week1Thursday.getTime()) / (7 * MS_PER_DAY),
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
  /** Partition-name format template (defaults to the period's standard suffix). */
  format?: string;
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
 * A partition-name suffix is a tiny template of literal characters and
 * `{PLACEHOLDER}` tokens. The placeholders available depend on the period, and
 * each resolves to the *same* date component pgslice already uses — so a custom
 * format only changes the rendered string, never which dates a partition
 * covers. For weeks the year/week are ISO (week-year + ISO week); for
 * day/month/year they are the calendar fields.
 *
 *   week:  {YYYY} = ISO week-year, {WW} = ISO week (01–53)   default "{YYYY}w{WW}"
 *   month: {YYYY} = year,          {MM} = month (01–12)      default "{YYYY}{MM}"
 *   day:   {YYYY}, {MM}, {DD}                                default "{YYYY}{MM}{DD}"
 *   year:  {YYYY}                                            default "{YYYY}"
 *
 * Examples: "p{YYYY}w{WW}" → "p2027w01", "y{YYYY}m{MM}" → "y2027m05".
 */
interface PlaceholderSpec {
  /** Renders the placeholder's value from a period-start date. */
  render: (date: Date) => string;
  /** Regex (without a capturing group) matching the rendered value. */
  pattern: string;
}

interface PeriodFormat {
  placeholders: Record<string, PlaceholderSpec>;
  defaultFormat: string;
  /** Rebuilds the period-start date from parsed placeholder values. */
  reconstruct: (values: Record<string, number>) => Date;
}

const pad = (value: number, width: number): string =>
  value.toString().padStart(width, "0");

function periodFormat(period: Period): PeriodFormat {
  const calendarYear: PlaceholderSpec = {
    render: (date) => pad(date.getUTCFullYear(), 4),
    pattern: "\\d{4}",
  };
  const calendarMonth: PlaceholderSpec = {
    render: (date) => pad(date.getUTCMonth() + 1, 2),
    pattern: "(?:0[1-9]|1[0-2])",
  };

  switch (period) {
    case "day":
      return {
        placeholders: {
          YYYY: calendarYear,
          MM: calendarMonth,
          DD: {
            render: (date) => pad(date.getUTCDate(), 2),
            pattern: "(?:0[1-9]|[12]\\d|3[01])",
          },
        },
        defaultFormat: "{YYYY}{MM}{DD}",
        reconstruct: (v) => new Date(Date.UTC(v.YYYY, v.MM - 1, v.DD)),
      };
    case "week":
      return {
        placeholders: {
          YYYY: {
            render: (date) => pad(isoWeekInfo(date).isoYear, 4),
            pattern: "\\d{4}",
          },
          WW: {
            render: (date) => pad(isoWeekInfo(date).isoWeek, 2),
            pattern: "(?:0[1-9]|[1-4]\\d|5[0-3])",
          },
        },
        defaultFormat: "{YYYY}w{WW}",
        reconstruct: (v) => isoWeekToMonday(v.YYYY, v.WW),
      };
    case "month":
      return {
        placeholders: { YYYY: calendarYear, MM: calendarMonth },
        defaultFormat: "{YYYY}{MM}",
        reconstruct: (v) => new Date(Date.UTC(v.YYYY, v.MM - 1, 1)),
      };
    case "year":
      return {
        placeholders: { YYYY: calendarYear },
        defaultFormat: "{YYYY}",
        reconstruct: (v) => new Date(Date.UTC(v.YYYY, 0, 1)),
      };
  }
}

type FormatSegment = { placeholder: string } | { literal: string };

const FORMAT_TOKEN = /\{([A-Z]+)\}|([^{}]+)/g;

/**
 * Splits a format template into ordered literal/placeholder segments,
 * validating that every placeholder is known for the period, every literal is
 * a safe identifier fragment, and every required placeholder appears exactly
 * once. Throws on a malformed template so a mistyped settings comment fails
 * loudly rather than producing an unusable partition name.
 */
function compileFormat(
  period: Period,
  template: string,
): { segments: FormatSegment[]; order: string[] } {
  // Literals are restricted to [a-z0-9] (no "_"): a partition is named
  // `<table>_<suffix>` and the suffix is recovered by splitting on the last
  // "_" (see parsePartitionDate), so an underscore inside the suffix template
  // would make the rendered name unparseable.
  if (!/^(?:[a-z0-9]+|\{[A-Z]+\})+$/.test(template)) {
    throw new Error(
      `Malformed ${period} partition format "${template}"; use [a-z0-9] literals and {PLACEHOLDER} tokens`,
    );
  }

  const { placeholders } = periodFormat(period);
  const segments: FormatSegment[] = [];
  const order: string[] = [];

  for (const match of template.matchAll(FORMAT_TOKEN)) {
    const [, placeholder, literal] = match;
    if (placeholder !== undefined) {
      if (!(placeholder in placeholders)) {
        throw new Error(
          `Unknown placeholder "{${placeholder}}" in ${period} partition format "${template}"`,
        );
      }
      if (order.includes(placeholder)) {
        throw new Error(
          `Duplicate placeholder "{${placeholder}}" in ${period} partition format "${template}"`,
        );
      }
      segments.push({ placeholder });
      order.push(placeholder);
    } else if (literal !== undefined) {
      segments.push({ literal });
    }
  }

  const missing = Object.keys(placeholders).filter(
    (name) => !order.includes(name),
  );
  if (missing.length > 0) {
    throw new Error(
      `${period} partition format "${template}" is missing required placeholder(s): ${missing
        .map((name) => `{${name}}`)
        .join(", ")}`,
    );
  }

  return { segments, order };
}

/**
 * Renders a partition-name suffix for the period-start `date`, using the given
 * format template (or the period's default suffix when omitted).
 */
export function formatDateSuffix(
  date: Date,
  period: Period,
  format?: string,
): string {
  const { placeholders, defaultFormat } = periodFormat(period);
  const { segments } = compileFormat(period, format ?? defaultFormat);
  return segments
    .map((segment) =>
      "literal" in segment
        ? segment.literal
        : placeholders[segment.placeholder].render(date),
    )
    .join("");
}

/**
 * Parses a partition table name back to its period-start date, inverting
 * {@link formatDateSuffix} for the same period + format. The suffix is the last
 * underscore-separated component. Throws if the suffix doesn't match the format
 * (e.g. a legacy-named or out-of-range partition), so misuse on a
 * differently-named table is immediately visible.
 */
export function parsePartitionDate(
  partitionName: string,
  period: Period,
  format?: string,
): Date {
  const suffix = partitionName.split("_").pop();
  if (!suffix) {
    throw new Error(`Invalid partition name: ${partitionName}`);
  }

  const { placeholders, defaultFormat, reconstruct } = periodFormat(period);
  const { segments, order } = compileFormat(period, format ?? defaultFormat);
  const regex = new RegExp(
    `^${segments
      .map((segment) =>
        "literal" in segment
          ? segment.literal
          : `(${placeholders[segment.placeholder].pattern})`,
      )
      .join("")}$`,
  );

  const match = suffix.match(regex);
  if (!match) {
    throw new Error(
      `Unrecognized ${period} partition suffix "${suffix}" in "${partitionName}"`,
    );
  }

  const values: Record<string, number> = {};
  order.forEach((placeholder, index) => {
    values[placeholder] = parseInt(match[index + 1], 10);
  });
  return reconstruct(values);
}

/**
 * An iterable that generates date ranges for partitions.
 */
export class DateRanges implements Iterable<DateRange> {
  readonly #today: Date;
  readonly #period: Period;
  readonly #past: number;
  readonly #future: number;
  readonly #format?: string;

  constructor(options: DateRangesOptions) {
    this.#today = options.today
      ? roundDate(options.today, options.period)
      : roundDate(new Date(), options.period);
    this.#period = options.period;
    this.#past = options.past;
    this.#future = options.future;
    this.#format = options.format;
  }

  *[Symbol.iterator](): Generator<DateRange> {
    for (let n = -this.#past; n <= this.#future; n++) {
      const start = advanceDate(this.#today, this.#period, n);
      const end = advanceDate(start, this.#period, 1);
      const suffix = formatDateSuffix(start, this.#period, this.#format);

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
  // Anchored: a date, an optional time, and an optional (UTC, redundant)
  // offset — and nothing else. Unexpected trailing content yields null rather
  // than a silently-truncated parse.
  const match = literal[1].match(
    /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?)(?:[+-]\d{2}(?::?\d{2})?)?)?$/,
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
 * Whether a Date sits exactly on a UTC-midnight boundary. Bounds-anchored
 * extension emits new boundaries at UTC midnight (see formatDateForSql), so it
 * can only abut existing partitions whose own boundaries are UTC-midnight.
 */
export function isUtcMidnight(date: Date): boolean {
  return (
    date.getUTCHours() === 0 &&
    date.getUTCMinutes() === 0 &&
    date.getUTCSeconds() === 0 &&
    date.getUTCMilliseconds() === 0
  );
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
  format?: string;
}): Generator<DateRange> {
  let start = options.anchorStart;
  for (
    let count = 0;
    start.getTime() <= options.horizon.getTime() &&
    count < MAX_GENERATED_PARTITIONS;
    count++
  ) {
    const end = advanceDate(start, options.period, 1);
    yield {
      start,
      end,
      suffix: formatDateSuffix(start, options.period, options.format),
    };
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
  format?: string;
}): Generator<DateRange> {
  let end = options.anchorEnd;
  for (let count = 0; count < MAX_GENERATED_PARTITIONS; count++) {
    const start = advanceDate(end, options.period, -1);
    // Inclusive of the horizon period (start >= horizon), mirroring the
    // start <= horizon guard in extendRanges for the opposite direction.
    if (start.getTime() < options.horizon.getTime()) {
      break;
    }
    yield {
      start,
      end,
      suffix: formatDateSuffix(start, options.period, options.format),
    };
    end = start;
  }
}
