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

    for (const part of parts) {
      const [key, value] = part.split(":");
      if (key === "column") {
        column = value;
      } else if (key === "period" && isValidPeriod(value)) {
        period = value;
      } else if (key === "cast" && isValidCast(value)) {
        cast = value;
      }
    }

    if (column && period && cast) {
      return new TableSettings(column, period, cast);
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
