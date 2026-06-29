import type { Cast, Period } from "./types.js";
import { PERIODS } from "./types.js";

/**
 * Settings stored in a partitioned table's comment.
 */
export class TableSettings {
  constructor(
    readonly column: string,
    readonly period: Period,
    readonly cast: Cast,
    /**
     * Optional partition-name format template (e.g. `p{YYYY}w{WW}`). When
     * absent, the period's standard suffix is used. Lets a retrofitted table
     * keep its existing naming convention; see {@link formatDateSuffix}.
     */
    readonly format?: string,
  ) {}

  /**
   * Parses table settings from a comment string.
   * Returns null if the comment doesn't contain valid settings.
   */
  static parseFromComment(comment: string): TableSettings | null {
    const parts = comment.split(",");
    let column: string | undefined;
    let period: Period | undefined;
    let cast: Cast | undefined;
    let format: string | undefined;

    for (const part of parts) {
      const [key, value] = part.split(":");
      if (key === "column") {
        column = value;
      } else if (key === "period" && isValidPeriod(value)) {
        period = value;
      } else if (key === "cast" && isValidCast(value)) {
        cast = value;
      } else if (key === "format" && value) {
        // Stored verbatim; validated when rendered/parsed (compileFormat), so a
        // mistyped template fails loudly at add_partitions time rather than here.
        format = value;
      }
    }

    if (column && period && cast) {
      return new TableSettings(column, period, cast, format);
    }

    return null;
  }
}

function isValidPeriod(value: string): value is Period {
  return PERIODS.includes(value as Period);
}

function isValidCast(value: string): value is Cast {
  return value === "date" || value === "timestamptz";
}
