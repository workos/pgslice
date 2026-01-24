import { sql } from "slonik";
import type { Cast } from "./types.js";

/**
 * Creates a SQL fragment from a raw string.
 * This is useful for dynamically building SQL queries where the content
 * is trusted (e.g., index definitions from the database).
 */
export function rawSql(query: string) {
  const raw = Object.freeze([query]);
  const strings = Object.assign([query], { raw });
  return sql.fragment(strings);
}

/**
 * Formats a Date as a SQL literal value for use in partition constraints.
 * Uses UTC values to ensure consistent behavior across timezones.
 */
export function formatDateForSql(date: Date, cast: Cast) {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");

  if (cast === "timestamptz") {
    return sql.literalValue(`${year}-${month}-${day} 00:00:00 UTC`);
  }
  return sql.literalValue(`${year}-${month}-${day}`);
}
