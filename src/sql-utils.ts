import { sql } from "slonik";

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
