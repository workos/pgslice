import { sql, type DatabaseTransactionConnection } from "slonik";
import { z } from "zod";

import type { Cast, Period } from "../types.js";

/**
 * Shared fixtures for exercising the partition-retrofit engine against the
 * range of real-world partitioned-table shapes: composite parent-owned primary
 * keys, weekly / monthly / year-resetting periods, `timestamp` vs `timestamptz`
 * keys, DEFAULT and MINVALUE catch-alls, schema-qualified tables, and a
 * replication-style role whose grants new partitions must inherit.
 *
 * The names here are deliberately generic — they encode the *shape*, not any
 * particular application's tables.
 */

export const TSTZ = sql.fragment`timestamp with time zone`;
export const TS = sql.fragment`timestamp without time zone`;
export const DATE = sql.fragment`date`;

/** Splits `schema.table` (defaulting to the `public` schema) into its parts. */
export function parts(qualified: string): [string, string] {
  const split = qualified.includes(".")
    ? qualified.split(".")
    : ["public", qualified];
  return [split[0], split[1]];
}

/** A SQL identifier token for a possibly schema-qualified table name. */
export const ident = (qualified: string) => sql.identifier(parts(qualified));

/** Creates the schema of a qualified name when it isn't `public`. */
export async function ensureSchema(
  tx: DatabaseTransactionConnection,
  qualified: string,
): Promise<void> {
  const [schema] = parts(qualified);
  if (schema !== "public") {
    await tx.query(
      sql.unsafe`CREATE SCHEMA IF NOT EXISTS ${sql.identifier([schema])}`,
    );
  }
}

/** Writes the pgslice settings comment that marks a table as managed. */
export async function setSettings(
  tx: DatabaseTransactionConnection,
  qualified: string,
  column: string,
  period: Period,
  cast: Cast,
): Promise<void> {
  await tx.query(
    sql.unsafe`COMMENT ON TABLE ${ident(qualified)} IS ${sql.literalValue(
      `column:${column},period:${period},cast:${cast},version:3`,
    )}`,
  );
}

export interface ParentOptions {
  /** Extra column definitions, e.g. ``sql.fragment`, session_id bigint, kind text` ``. */
  extraColumns?: ReturnType<typeof sql.fragment>;
  /** Columns for an extra parent-level UNIQUE constraint. */
  unique?: string[];
  /** A parent-level CHECK expression, e.g. ``sql.fragment`kind <> ''` ``. */
  check?: ReturnType<typeof sql.fragment>;
}

/**
 * Native parent: the partitioned parent owns the composite primary key, so
 * Postgres propagates it (and any partitioned indexes) to each partition.
 */
export async function nativeParent(
  tx: DatabaseTransactionConnection,
  qualified: string,
  column: string,
  columnType: ReturnType<typeof sql.fragment>,
  period: Period,
  cast: Cast,
  options?: ParentOptions,
): Promise<void> {
  await ensureSchema(tx, qualified);
  const extraColumns = options?.extraColumns ?? sql.fragment``;
  const unique = options?.unique
    ? sql.fragment`,
        UNIQUE (${sql.join(
          options.unique.map((c) => sql.identifier([c])),
          sql.fragment`, `,
        )})`
    : sql.fragment``;
  const check = options?.check
    ? sql.fragment`,
        CHECK (${options.check})`
    : sql.fragment``;
  await tx.query(sql.unsafe`
    CREATE TABLE ${ident(qualified)} (
      id bigint NOT NULL,
      ${sql.identifier([column])} ${columnType} NOT NULL,
      payload text${extraColumns},
      PRIMARY KEY (id, ${sql.identifier([column])})${unique}${check}
    ) PARTITION BY RANGE (${sql.identifier([column])})
  `);
  await setSettings(tx, qualified, column, period, cast);
}

/**
 * Classic pgslice parent: no primary key on the parent; each partition owns its
 * own.
 */
export async function pgsliceParent(
  tx: DatabaseTransactionConnection,
  qualified: string,
  column: string,
  columnType: ReturnType<typeof sql.fragment>,
  period: Period,
  cast: Cast,
): Promise<void> {
  await ensureSchema(tx, qualified);
  await tx.query(sql.unsafe`
    CREATE TABLE ${ident(qualified)} (
      id bigint NOT NULL,
      ${sql.identifier([column])} ${columnType} NOT NULL,
      payload text
    ) PARTITION BY RANGE (${sql.identifier([column])})
  `);
  await setSettings(tx, qualified, column, period, cast);
}

/** Attaches a bounded range partition, optionally adding a per-child PK. */
export async function addChild(
  tx: DatabaseTransactionConnection,
  parentQualified: string,
  childName: string,
  from: string,
  to: string,
  childPrimaryKey?: string[],
): Promise<void> {
  const [schema] = parts(parentQualified);
  const childQualified = `${schema}.${childName}`;
  await tx.query(sql.unsafe`
    CREATE TABLE ${ident(childQualified)}
    PARTITION OF ${ident(parentQualified)}
    FOR VALUES FROM (${sql.literalValue(from)}) TO (${sql.literalValue(to)})
  `);
  if (childPrimaryKey) {
    await tx.query(sql.unsafe`
      ALTER TABLE ${ident(childQualified)}
      ADD PRIMARY KEY (${sql.join(
        childPrimaryKey.map((c) => sql.identifier([c])),
        sql.fragment`, `,
      )})
    `);
  }
}

/** Attaches the DEFAULT catch-all partition. */
export async function addDefault(
  tx: DatabaseTransactionConnection,
  parentQualified: string,
  childName: string,
): Promise<void> {
  const [schema] = parts(parentQualified);
  await tx.query(sql.unsafe`
    CREATE TABLE ${ident(`${schema}.${childName}`)}
    PARTITION OF ${ident(parentQualified)} DEFAULT
  `);
}

/** Attaches an unbounded-below (`MINVALUE`) historic partition. */
export async function addMinvalueChild(
  tx: DatabaseTransactionConnection,
  parentQualified: string,
  childName: string,
  to: string,
): Promise<void> {
  const [schema] = parts(parentQualified);
  await tx.query(sql.unsafe`
    CREATE TABLE ${ident(`${schema}.${childName}`)}
    PARTITION OF ${ident(parentQualified)}
    FOR VALUES FROM (MINVALUE) TO (${sql.literalValue(to)})
  `);
}

/**
 * Creates a replication-style role idempotently. Roles are cluster-global, so
 * pass a name unique to the test file to avoid colliding with the parallel
 * suite — the guarded `IF NOT EXISTS` alone can still race between concurrent
 * transactions.
 */
export async function createCdcRole(
  tx: DatabaseTransactionConnection,
  roleName: string,
): Promise<void> {
  await tx.query(sql.unsafe`
    DO $$ BEGIN
      IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = ${sql.literalValue(
        roleName,
      )}) THEN
        CREATE ROLE ${sql.identifier([roleName])} NOLOGIN;
      END IF;
    END $$;
  `);
}

/** Returns the names of a partitioned table's child partitions, sorted. */
export async function childNames(
  tx: DatabaseTransactionConnection,
  qualified: string,
): Promise<string[]> {
  const [schema, name] = parts(qualified);
  const rows = await tx.any(
    sql.type(z.object({ name: z.string() }))`
      SELECT child.relname AS name
      FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace np ON np.oid = parent.relnamespace
      WHERE np.nspname = ${schema} AND parent.relname = ${name}
      ORDER BY child.relname
    `,
  );
  return rows.map((r) => r.name);
}

/**
 * Returns each child partition's name and its `pg_get_expr(relpartbound)` text,
 * sorted by name. Pins the session to UTC first so `timestamptz` bounds render
 * deterministically as `+00`.
 */
export async function childBounds(
  tx: DatabaseTransactionConnection,
  qualified: string,
): Promise<readonly { name: string; bound: string }[]> {
  await tx.query(sql.unsafe`SET LOCAL TIME ZONE 'UTC'`);
  const [schema, name] = parts(qualified);
  return tx.any(
    sql.type(z.object({ name: z.string(), bound: z.string() }))`
      SELECT child.relname AS name, pg_get_expr(child.relpartbound, child.oid) AS bound
      FROM pg_inherits i
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace np ON np.oid = parent.relnamespace
      WHERE np.nspname = ${schema} AND parent.relname = ${name}
      ORDER BY child.relname
    `,
  );
}

/**
 * Asserts contiguity (no gap, no overlap) over a set of partition bounds.
 * Only finite `FROM ('..') TO ('..')` bounds participate: MINVALUE/MAXVALUE and
 * DEFAULT partitions have no finite endpoint and are ignored, so callers that
 * want them considered must assert those edges separately.
 */
export function assertContiguous(
  bounds: readonly { name: string; bound: string }[],
): void {
  // Bounds are read under a UTC-pinned session (see childBounds), so a
  // timestamptz renders as `...+00` and a timestamp/date without an offset —
  // both UTC. Parse the date/time components and treat them as UTC; appending
  // `Z` to a `+00` string would be invalid ISO 8601 (NaN).
  const toMs = (token: string): number => {
    const date = token.match(/\d{4}-\d{2}-\d{2}/)?.[0];
    const time = token.match(/\d{2}:\d{2}:\d{2}/)?.[0] ?? "00:00:00";
    return date ? Date.parse(`${date}T${time}Z`) : NaN;
  };
  const ranges = bounds
    .map((r) => r.bound.match(/FROM \('(.+?)'\) TO \('(.+?)'\)/))
    .flatMap((m) => (m ? [{ lo: toMs(m[1]), hi: toMs(m[2]) }] : []))
    .sort((a, b) => a.lo - b.lo);
  for (let i = 1; i < ranges.length; i++) {
    if (ranges[i].lo !== ranges[i - 1].hi) {
      throw new Error(
        `partitions are not contiguous near index ${i}: ${ranges[i - 1].hi} != ${ranges[i].lo}`,
      );
    }
  }
}
