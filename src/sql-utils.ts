import {
  sql,
  type PrimitiveValueExpression,
  type SerializableValue,
} from "slonik";
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

/**
 * Converts a value to a SQL fragment using the appropriate slonik helper based on data type.
 */
export function valueToSql(val: unknown, dataType: string) {
  if (val === null) {
    return sql.fragment`NULL`;
  }

  if (
    dataType === "timestamp with time zone" ||
    dataType === "timestamp without time zone"
  ) {
    if (typeof val === "number") {
      return sql.timestamp(new Date(val));
    }
    if (val instanceof Date) {
      return sql.timestamp(val);
    }
  }

  if (dataType === "date") {
    if (typeof val === "string") {
      return sql.date(new Date(val));
    }
    if (val instanceof Date) {
      return sql.date(val);
    }
  }

  if (dataType === "uuid" && typeof val === "string") {
    return sql.uuid(val);
  }

  if (dataType === "bytea" && Buffer.isBuffer(val)) {
    return sql.binary(val);
  }

  if (dataType === "jsonb" && typeof val === "object") {
    return sql.jsonb(val as SerializableValue);
  }

  if (dataType === "json" && typeof val === "object") {
    return sql.json(val as SerializableValue);
  }

  return sql.fragment`${val as PrimitiveValueExpression}`;
}
