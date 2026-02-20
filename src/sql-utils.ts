import {
  createSqlTag,
  type CommonQueryMethods,
  type PrimitiveValueExpression,
  type SerializableValue,
} from "slonik";
import { z } from "zod";

import type { Cast } from "./types.js";

export const sql = createSqlTag({
  typeAliases: {
    void: z.object({}).strict(),
  },
});

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

const statementTimeoutSchema = z.object({
  statement_timeout: z.string(),
  statement_timeout_ms: z.coerce.number(),
});

export async function withStatementTimeout<T>(
  connection: CommonQueryMethods,
  minTimeoutMs: number | undefined,
  handler: () => Promise<T>,
): Promise<T> {
  if (!minTimeoutMs || minTimeoutMs <= 0) {
    return handler();
  }

  const settings = await connection.one(
    sql.type(statementTimeoutSchema)`
      SELECT
        current_setting('statement_timeout') AS statement_timeout,
        CASE
          WHEN current_setting('statement_timeout') IN ('0', '0ms') THEN 0
          ELSE (EXTRACT(EPOCH FROM current_setting('statement_timeout')::interval) * 1000)::bigint
        END AS statement_timeout_ms
    `,
  );

  if (
    settings.statement_timeout_ms === 0 ||
    settings.statement_timeout_ms >= minTimeoutMs
  ) {
    return handler();
  }

  await connection.query(
    sql.typeAlias(
      "void",
    )`SELECT set_config('statement_timeout', ${String(minTimeoutMs)}, true)`,
  );
  try {
    return await handler();
  } finally {
    try {
      await connection.query(
        sql.typeAlias(
          "void",
        )`SELECT set_config('statement_timeout', ${settings.statement_timeout}, true)`,
      );
    } catch {
      // Ignore errors to avoid masking the original failure.
    }
  }
}
