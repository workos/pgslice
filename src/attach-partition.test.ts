import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  test,
} from "vitest";
import {
  createPool,
  sql,
  type CommonQueryMethods,
  type DatabasePool,
  type QuerySqlToken,
} from "slonik";
import { z } from "zod";

import { Pgslice } from "./pgslice.js";

/**
 * Lock-behaviour and catalog-parity tests for partition creation.
 *
 * Unlike the rest of the suite these can't run inside a rolled-back test
 * transaction: they need committed tables and several concurrent connections
 * (one holding locks, one running pgslice, one acting as application
 * traffic). They run in a throwaway database so nothing they leave behind can
 * collide with the transactional tests running in parallel.
 */

const NOW = new Date(Date.UTC(2026, 0, 15));
const DATABASE = `pgslice_attach_${process.pid}`;
const READER = "pgslice_attach_reader";
const WRITER = "pgslice_attach_writer";

function databaseUrl(database: string, applicationName: string): string {
  const url = new URL(process.env.PGSLICE_URL ?? "");
  url.pathname = `/${database}`;
  url.searchParams.set("application_name", applicationName);
  return url.toString();
}

const pgsliceUrl = () => new URL(databaseUrl(DATABASE, "pgslice"));

let admin: DatabasePool;
let pool: DatabasePool;
let pgslice: Pgslice;

beforeAll(async () => {
  admin = await createPool(
    databaseUrl(new URL(process.env.PGSLICE_URL ?? "").pathname.slice(1), "x"),
  );
  await admin.query(
    sql.unsafe`DROP DATABASE IF EXISTS ${sql.identifier([DATABASE])}`,
  );
  await admin.query(sql.unsafe`CREATE DATABASE ${sql.identifier([DATABASE])}`);
  for (const role of [READER, WRITER]) {
    await admin.query(sql.unsafe`
      DO $$ BEGIN
        CREATE ROLE ${sql.identifier([role])};
      EXCEPTION WHEN duplicate_object THEN NULL;
      END $$
    `);
  }
  pool = await createPool(databaseUrl(DATABASE, "attach-test"), {
    maximumPoolSize: 10,
  });
});

afterAll(async () => {
  await pool?.end();
  await admin?.query(
    sql.unsafe`DROP DATABASE IF EXISTS ${sql.identifier([DATABASE])}`,
  );
  await admin?.end();
});

beforeEach(async () => {
  await pool.query(sql.unsafe`DROP SCHEMA IF EXISTS t CASCADE`);
  await pool.query(sql.unsafe`CREATE SCHEMA t`);
  pgslice = await Pgslice.connect(pgsliceUrl());
});

afterEach(async () => {
  await pgslice.close();
});

class Rollback extends Error {}

/**
 * Runs `statements` in a transaction on its own connection and keeps that
 * transaction open (holding whatever locks the statements took) until the
 * returned release function is called, which rolls it back.
 */
async function holdOpen(
  ...statements: QuerySqlToken[]
): Promise<() => Promise<void>> {
  let release: () => void = () => {};
  const released = new Promise<void>((resolve) => (release = resolve));
  let ready: (error?: unknown) => void = () => {};
  const held = new Promise<void>((resolve, reject) => {
    ready = (error) => (error ? reject(error) : resolve());
  });

  const done = pool
    .transaction(async (tx) => {
      try {
        for (const statement of statements) {
          await tx.query(statement);
        }
      } catch (error) {
        ready(error);
        throw error;
      }
      ready();
      await released;
      throw new Rollback();
    })
    .catch((error) => {
      if (!(error instanceof Rollback)) throw error;
    });

  await held;
  return async () => {
    release();
    await done;
  };
}

async function waitFor<T>(
  probe: () => Promise<T | undefined>,
  timeoutMs = 5000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await probe();
    if (value !== undefined) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("Timed out waiting for condition");
}

const LockRow = z.object({
  relation: z.string(),
  mode: z.string(),
  granted: z.boolean(),
});

/** Relation locks held or awaited by the pgslice connection. */
async function pgsliceLocks(): Promise<readonly z.infer<typeof LockRow>[]> {
  return pool.any(sql.type(LockRow)`
    SELECT l.relation::regclass::text AS relation, l.mode, l.granted
    FROM pg_locks l
    JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE a.application_name = 'pgslice'
      AND a.datname = current_database()
      AND l.locktype = 'relation'
      AND l.relation::regclass::text LIKE 't.%'
    ORDER BY 1, 2
  `);
}

/** Waits until pgslice is blocked and returns the lock it is waiting for. */
async function waitingLock() {
  return waitFor(async () => (await pgsliceLocks()).find((l) => !l.granted));
}

async function pgslicePid(): Promise<number> {
  return pool.oneFirst(sql.type(z.object({ pid: z.number() }))`
    SELECT pid FROM pg_stat_activity
    WHERE application_name = 'pgslice' AND datname = current_database()
  `);
}

async function relationsLike(pattern: string): Promise<readonly string[]> {
  return pool.anyFirst(sql.type(z.object({ name: z.string() }))`
    SELECT relname AS name FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 't' AND relname LIKE ${pattern}
    ORDER BY 1
  `);
}

async function partitionsOf(parent: string): Promise<readonly string[]> {
  return pool.anyFirst(sql.type(z.object({ name: z.string() }))`
    SELECT inhrelid::regclass::text AS name FROM pg_inherits
    WHERE inhparent = ${sql.literalValue(parent)}::regclass
    ORDER BY 1
  `);
}

async function addPartitions(
  table: string,
  extra: { lockTimeout?: string; future?: number; tablespace?: string } = {},
): Promise<string[]> {
  return pgslice.start((connection) =>
    pgslice.addPartitions(connection, {
      table,
      future: extra.future ?? 1,
      now: NOW,
      lockTimeout: extra.lockTimeout,
      tablespace: extra.tablespace,
    }),
  );
}

/** The partition-creation statement pgslice used before switching to ATTACH. */
function partitionOfStatement(name: string, from: string, to: string) {
  return sql.unsafe`
    CREATE TABLE ${sql.identifier(["t", name])}
    PARTITION OF t.posts
    FOR VALUES FROM (${sql.literalValue(from)}) TO (${sql.literalValue(to)})
  `;
}

/**
 * A monthly parent shaped like a typical natively-partitioned table: a
 * composite primary key owned by the parent, an outbound foreign key, a
 * partitioned secondary index, and one existing partition (2026-01).
 */
async function createPosts({ outboundFk = false } = {}) {
  await pool.query(sql.unsafe`CREATE TABLE t.authors (id int PRIMARY KEY)`);
  await pool.query(sql.unsafe`INSERT INTO t.authors VALUES (1), (2)`);
  await pool.query(sql.unsafe`
    CREATE TABLE t.posts (
      id bigint NOT NULL,
      author_id int NOT NULL
        ${outboundFk ? sql.fragment`REFERENCES t.authors (id)` : sql.fragment``},
      created_at timestamp NOT NULL,
      PRIMARY KEY (id, created_at)
    ) PARTITION BY RANGE (created_at)
  `);
  await pool.query(sql.unsafe`CREATE INDEX ON t.posts (author_id)`);
  await pool.query(
    sql.unsafe`COMMENT ON TABLE t.posts IS 'column:created_at,period:month,cast:date,version:3'`,
  );
  await pool.query(sql.unsafe`
    CREATE TABLE t.posts_202601 PARTITION OF t.posts
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
  `);
  await pool.query(sql.unsafe`INSERT INTO t.posts VALUES (1, 1, '2026-01-05')`);
}

describe("partition creation lock behaviour", { timeout: 30_000 }, () => {
  test("succeeds while another transaction holds an uncommitted write on the parent", async () => {
    await createPosts();
    const release = await holdOpen(
      sql.unsafe`INSERT INTO t.posts VALUES (2, 1, '2026-01-06')`,
    );
    try {
      const started = Date.now();
      expect(await addPartitions("t.posts", { lockTimeout: "2s" })).toEqual([
        "posts_202602",
      ]);
      expect(Date.now() - started).toBeLessThan(2000);
      expect(await partitionsOf("t.posts")).toEqual([
        "t.posts_202601",
        "t.posts_202602",
      ]);
    } finally {
      await release();
    }
  });

  test("regression guard: CREATE TABLE ... PARTITION OF blocks behind the same write", async () => {
    await createPosts();
    const release = await holdOpen(
      sql.unsafe`DELETE FROM t.posts WHERE id = 1`,
    );
    try {
      await expect(
        pool.transaction(async (tx) => {
          await tx.query(sql.unsafe`SET LOCAL lock_timeout = '500ms'`);
          await tx.query(
            partitionOfStatement("posts_202602", "2026-02-01", "2026-03-01"),
          );
        }),
      ).rejects.toThrow(/lock timeout/);
    } finally {
      await release();
    }
  });

  test("succeeds while another transaction holds ACCESS SHARE on the parent", async () => {
    await createPosts();
    const release = await holdOpen(
      sql.unsafe`DECLARE c CURSOR FOR SELECT * FROM t.posts`,
      sql.unsafe`FETCH 1 FROM c`,
    );
    try {
      expect(await addPartitions("t.posts", { lockTimeout: "2s" })).toEqual([
        "posts_202602",
      ]);
    } finally {
      await release();
    }
  });

  test.each([
    {
      label: "LOCK TABLE ... SHARE UPDATE EXCLUSIVE",
      statement: sql.unsafe`LOCK TABLE t.posts IN SHARE UPDATE EXCLUSIVE MODE`,
    },
    { label: "an open ANALYZE", statement: sql.unsafe`ANALYZE t.posts` },
  ])(
    "times out behind $label, reports the table as failed and leaves nothing behind",
    async ({ statement }) => {
      await createPosts();
      const before = await relationsLike("posts%");
      const release = await holdOpen(statement);
      try {
        const results = await pgslice.start((connection) =>
          pgslice.maintain(connection, {
            schema: "t",
            futureMonthly: 2,
            lockTimeout: "500ms",
            now: NOW,
          }),
        );
        expect(results).toHaveLength(1);
        expect(results[0].table).toBe("t.posts");
        expect(results[0].partitionsCreated).toEqual([]);
        expect(results[0].error).toMatch(/lock timeout/);
      } finally {
        await release();
      }

      expect(await relationsLike("posts%")).toEqual(before);
      expect(await partitionsOf("t.posts")).toEqual(["t.posts_202601"]);
      const constraints = await pool.oneFirst(
        sql.type(z.object({ count: z.number() }))`
          SELECT count(*)::int AS count FROM pg_constraint c
          JOIN pg_namespace n ON n.oid = c.connamespace
          WHERE n.nspname = 't' AND conname LIKE 'posts_2026%'
            AND conname NOT LIKE 'posts_202601%'
        `,
      );
      expect(constraints).toBe(0);
    },
  );

  test("waits for SHARE UPDATE EXCLUSIVE (not ACCESS EXCLUSIVE) on the parent while application writes proceed", async () => {
    await createPosts();
    const release = await holdOpen(
      sql.unsafe`LOCK TABLE t.posts IN SHARE UPDATE EXCLUSIVE MODE`,
    );
    let pending: Promise<string[]> | undefined;
    try {
      pending = addPartitions("t.posts", { lockTimeout: "10s" });
      const waiting = await waitingLock();
      expect(waiting).toEqual({
        relation: "t.posts",
        mode: "ShareUpdateExclusiveLock",
        granted: false,
      });
      const locks = await pgsliceLocks();
      expect(
        locks.filter(
          (l) => l.relation === "t.posts" && l.mode === "AccessExclusiveLock",
        ),
      ).toEqual([]);

      // Application traffic through a third connection is not queued behind
      // the waiting ATTACH.
      await pool.transaction(async (tx) => {
        await tx.query(sql.unsafe`SET LOCAL statement_timeout = '2s'`);
        await tx.query(
          sql.unsafe`INSERT INTO t.posts VALUES (3, 1, '2026-01-07')`,
        );
        await tx.query(sql.unsafe`SELECT count(*) FROM t.posts`);
      });
    } finally {
      await release();
    }
    expect(await pending).toEqual(["posts_202602"]);
    expect(
      await pool.oneFirst(
        sql.type(z.object({ count: z.number() }))`
          SELECT count(*)::int AS count FROM t.posts
        `,
      ),
    ).toBe(2);
  });

  test("a statement cancelled between create and attach rolls the new table back", async () => {
    await createPosts();
    const release = await holdOpen(
      sql.unsafe`LOCK TABLE t.posts IN SHARE UPDATE EXCLUSIVE MODE`,
    );
    try {
      const pending = addPartitions("t.posts", { lockTimeout: "10s" });
      await waitingLock();
      // The standalone table already exists inside pgslice's transaction.
      await pool.query(
        sql.unsafe`SELECT pg_cancel_backend(${await pgslicePid()})`,
      );
      await expect(pending).rejects.toThrow(/cancel/i);
    } finally {
      await release();
    }
    expect(await relationsLike("posts_2026%")).toEqual([
      "posts_202601",
      "posts_202601_author_id_idx",
      "posts_202601_pkey",
    ]);
  });

  test("refuses to adopt an existing table that is not attached", async () => {
    await createPosts();
    await pool.query(
      sql.unsafe`CREATE TABLE t.posts_202602 (LIKE t.posts INCLUDING ALL)`,
    );
    await expect(addPartitions("t.posts", { future: 2 })).rejects.toThrow(
      "t.posts_202602 already exists but is not a partition of t.posts",
    );
    expect(await partitionsOf("t.posts")).toEqual(["t.posts_202601"]);
  });

  test("an outbound foreign key still needs SHARE ROW EXCLUSIVE on the referenced table", async () => {
    // Cloning the parent's foreign key onto the new partition locks the
    // referenced table, so a writer there still blocks partition creation.
    // Writes to the partitioned table itself keep flowing.
    await createPosts({ outboundFk: true });
    const release = await holdOpen(
      sql.unsafe`INSERT INTO t.authors VALUES (3)`,
    );
    try {
      const pending = addPartitions("t.posts", { lockTimeout: "1s" });
      expect(await waitingLock()).toEqual({
        relation: "t.authors",
        mode: "ShareRowExclusiveLock",
        granted: false,
      });
      expect(await pgsliceLocks()).toContainEqual({
        relation: "t.posts",
        mode: "ShareUpdateExclusiveLock",
        granted: true,
      });
      await pool.transaction(async (tx) => {
        await tx.query(sql.unsafe`SET LOCAL statement_timeout = '500ms'`);
        await tx.query(
          sql.unsafe`INSERT INTO t.posts VALUES (4, 1, '2026-01-08')`,
        );
      });
      await expect(pending).rejects.toThrow(/lock timeout/);
    } finally {
      await release();
    }
    expect(await relationsLike("posts_202602%")).toEqual([]);
  });
});

describe("DEFAULT partition", { timeout: 30_000 }, () => {
  async function createEvents() {
    await pool.query(sql.unsafe`
      CREATE TABLE t.events (
        id bigint NOT NULL,
        created_at timestamptz NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    await pool.query(
      sql.unsafe`COMMENT ON TABLE t.events IS 'column:created_at,period:month,cast:timestamptz,version:3'`,
    );
    await pool.query(sql.unsafe`
      CREATE TABLE t.events_202601 PARTITION OF t.events
      FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00')
    `);
    await pool.query(
      sql.unsafe`CREATE TABLE t.events_default PARTITION OF t.events DEFAULT`,
    );
  }

  test("attaches next to an empty DEFAULT partition", async () => {
    await createEvents();
    expect(await addPartitions("t.events")).toEqual(["events_202602"]);
    expect(await partitionsOf("t.events")).toEqual([
      "t.events_202601",
      "t.events_202602",
      "t.events_default",
    ]);
  });

  test("takes ACCESS EXCLUSIVE on the DEFAULT partition (but not the parent)", async () => {
    await createEvents();
    const release = await holdOpen(
      sql.unsafe`DECLARE c CURSOR FOR SELECT * FROM t.events_default`,
      sql.unsafe`FETCH 1 FROM c`,
    );
    try {
      const pending = addPartitions("t.events", { lockTimeout: "1s" });
      expect(await waitingLock()).toEqual({
        relation: "t.events_default",
        mode: "AccessExclusiveLock",
        granted: false,
      });
      expect(await pgsliceLocks()).toContainEqual({
        relation: "t.events",
        mode: "ShareUpdateExclusiveLock",
        granted: true,
      });
      await expect(pending).rejects.toThrow(/lock timeout/);
    } finally {
      await release();
    }
    expect(await relationsLike("events_202602%")).toEqual([]);
  });

  test("fails cleanly when the DEFAULT partition holds rows in the new range", async () => {
    await createEvents();
    await pool.query(
      sql.unsafe`INSERT INTO t.events VALUES (1, '2026-02-10 00:00:00+00')`,
    );
    await expect(addPartitions("t.events")).rejects.toThrow();
    expect(await relationsLike("events_202602%")).toEqual([]);
    expect(await partitionsOf("t.events")).toEqual([
      "t.events_202601",
      "t.events_default",
    ]);
    expect(
      await pool.oneFirst(
        sql.type(z.object({ count: z.number() }))`
          SELECT count(*)::int AS count FROM t.events_default
        `,
      ),
    ).toBe(1);
  });
});

/**
 * Snapshot of everything about a partition that should not depend on whether
 * it was created with PARTITION OF or LIKE + ATTACH. Every occurrence of the
 * partition's own name is replaced so two partitions can be compared, as is
 * the counter Postgres appends to the name of each foreign key it clones onto
 * a referencing table, one per partition (`..._fkey1` on PG13, `..._fkey_1`
 * on newer versions).
 */
async function partitionSnapshot(db: CommonQueryMethods, name: string) {
  const rel = sql.literalValue(`t.${name}`);
  const Json = z.object({ value: z.unknown() });
  const one = async (query: QuerySqlToken<typeof Json>) =>
    JSON.parse(
      JSON.stringify((await db.one(query)).value)
        .replaceAll(name, "<p>")
        .replace(/_fkey_?\d+"/g, '_fkey<n>"'),
    );

  return {
    class: await one(sql.type(Json)`
      SELECT jsonb_build_object(
        'relowner', relowner::regrole::text,
        'relacl', relacl::text[],
        'reloptions', reloptions,
        'relkind', relkind,
        'relpersistence', relpersistence,
        'relreplident', relreplident,
        'reltablespace', reltablespace,
        'relispartition', relispartition,
        'relnatts', relnatts,
        'relchecks', relchecks,
        'relhasindex', relhasindex,
        'relhastriggers', relhastriggers,
        'relrowsecurity', relrowsecurity,
        'parent', (SELECT inhparent::regclass::text FROM pg_inherits WHERE inhrelid = c.oid),
        'comment', obj_description(c.oid, 'pg_class'),
        'statistics', (SELECT count(*) FROM pg_statistic_ext WHERE stxrelid = c.oid)
      ) AS value
      FROM pg_class c WHERE c.oid = ${rel}::regclass
    `),
    attributes: await one(sql.type(Json)`
      SELECT jsonb_agg(
        (to_jsonb(a) - 'attrelid')
          || jsonb_build_object(
            'default', pg_get_expr(d.adbin, d.adrelid),
            'comment', col_description(a.attrelid, a.attnum)
          )
        ORDER BY a.attnum
      ) AS value
      FROM pg_attribute a
      LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
      WHERE a.attrelid = ${rel}::regclass AND a.attnum > 0
    `),
    constraints: await one(sql.type(Json)`
      SELECT coalesce(jsonb_agg(x ORDER BY x::text), '[]') AS value FROM (
        SELECT jsonb_build_object(
          'conname', c.conname,
          'contype', c.contype,
          'conrelid', c.conrelid::regclass::text,
          'confrelid', c.confrelid::regclass::text,
          'def', pg_get_constraintdef(c.oid),
          'condeferrable', c.condeferrable,
          'convalidated', c.convalidated,
          'conislocal', c.conislocal,
          'coninhcount', c.coninhcount,
          'connoinherit', c.connoinherit,
          'parent', (SELECT p.conname FROM pg_constraint p WHERE p.oid = c.conparentid),
          'index', c.conindid::regclass::text,
          'comment', obj_description(c.oid, 'pg_constraint')
        ) AS x
        FROM pg_constraint c
        WHERE c.conrelid = ${rel}::regclass OR c.confrelid = ${rel}::regclass
      ) s
    `),
    indexes: await one(sql.type(Json)`
      SELECT coalesce(jsonb_agg(x ORDER BY x::text), '[]') AS value FROM (
        SELECT jsonb_build_object(
          'def', pg_get_indexdef(i.indexrelid),
          'indisprimary', i.indisprimary,
          'indisunique', i.indisunique,
          'indisreplident', i.indisreplident,
          'indisvalid', i.indisvalid,
          'indimmediate', i.indimmediate,
          'reltablespace', ic.reltablespace,
          'reloptions', ic.reloptions,
          'parent', (SELECT inhparent::regclass::text FROM pg_inherits WHERE inhrelid = i.indexrelid)
        ) AS x
        FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE i.indrelid = ${rel}::regclass
      ) s
    `),
    // Triggers on the partition plus the foreign-key triggers on other tables
    // that exist because of it. Internal trigger names embed OIDs, so they are
    // identified by their function, type and owning constraint instead.
    triggers: await one(sql.type(Json)`
      SELECT coalesce(jsonb_agg(x ORDER BY x::text), '[]') AS value FROM (
        SELECT jsonb_build_object(
          'table', t.tgrelid::regclass::text,
          'name', CASE WHEN t.tgisinternal THEN NULL ELSE t.tgname END,
          'function', t.tgfoid::regproc::text,
          'type', t.tgtype,
          'enabled', t.tgenabled,
          'internal', t.tgisinternal,
          'constraint', c.conname,
          'constrrel', t.tgconstrrelid::regclass::text,
          'deferrable', t.tgdeferrable,
          'hasParent', t.tgparentid <> 0
        ) AS x
        FROM pg_trigger t
        LEFT JOIN pg_constraint c ON c.oid = t.tgconstraint
        WHERE t.tgrelid = ${rel}::regclass
          OR c.conrelid = ${rel}::regclass
          OR c.confrelid = ${rel}::regclass
      ) s
    `),
  };
}

describe(
  "schema parity with CREATE TABLE ... PARTITION OF",
  { timeout: 30_000 },
  () => {
    async function grantToRoles(table: string) {
      const ident = sql.identifier(["t", table]);
      await pool.query(
        sql.unsafe`GRANT SELECT ON ${ident} TO ${sql.identifier([READER])}`,
      );
      await pool.query(
        sql.unsafe`GRANT SELECT, INSERT, UPDATE ON ${ident} TO ${sql.identifier([WRITER])}`,
      );
      await pool.query(sql.unsafe`GRANT SELECT ON ${ident} TO PUBLIC`);
    }

    test("parent-owned composite primary key with FKs, trigger, checks, defaults and grants", async () => {
      await pool.query(sql.unsafe`CREATE TABLE t.authors (id int PRIMARY KEY)`);
      await pool.query(sql.unsafe`
      CREATE TABLE t.posts (
        id varchar NOT NULL,
        author_id int NOT NULL REFERENCES t.authors (id) ON DELETE CASCADE,
        status text NOT NULL DEFAULT 'new' CHECK (status <> ''),
        score int CHECK (score >= 0),
        double_score int GENERATED ALWAYS AS (score * 2) STORED,
        body text,
        created_at timestamp NOT NULL DEFAULT now(),
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
      await pool.query(
        sql.unsafe`ALTER TABLE t.posts ALTER COLUMN body SET STORAGE EXTERNAL`,
      );
      await pool.query(
        sql.unsafe`ALTER TABLE t.posts ALTER COLUMN score SET STATISTICS 500`,
      );
      await pool.query(
        sql.unsafe`COMMENT ON COLUMN t.posts.body IS 'the body'`,
      );
      await pool.query(
        sql.unsafe`CREATE STATISTICS t.posts_stats ON author_id, status FROM t.posts`,
      );
      await pool.query(sql.unsafe`CREATE INDEX ON t.posts (author_id)`);
      await pool.query(
        sql.unsafe`CREATE UNIQUE INDEX ON t.posts (body, created_at) WHERE body IS NOT NULL`,
      );
      await pool.query(sql.unsafe`
      CREATE FUNCTION t.touch() RETURNS trigger LANGUAGE plpgsql AS
      $$ BEGIN RETURN NEW; END $$
    `);
      await pool.query(sql.unsafe`
      CREATE TRIGGER posts_touch AFTER INSERT ON t.posts
      FOR EACH ROW EXECUTE FUNCTION t.touch()
    `);
      await pool.query(sql.unsafe`
      CREATE TABLE t.flags (
        post_id varchar,
        post_created_at timestamp,
        FOREIGN KEY (post_id, post_created_at) REFERENCES t.posts (id, created_at)
      )
    `);
      await grantToRoles("posts");
      await pool.query(
        sql.unsafe`COMMENT ON TABLE t.posts IS 'column:created_at,period:month,cast:date,version:3'`,
      );

      await pool.query(
        partitionOfStatement("posts_202601", "2026-01-01", "2026-02-01"),
      );
      await grantToRoles("posts_202601");

      expect(await addPartitions("t.posts")).toEqual(["posts_202602"]);

      const old = await partitionSnapshot(pool, "posts_202601");
      const attached = await partitionSnapshot(pool, "posts_202602");
      expect(attached).toEqual(old);
      // Sanity-check that the snapshot really captured the interesting parts.
      expect(old.constraints.length).toBeGreaterThanOrEqual(5);
      expect(old.indexes).toHaveLength(3);
      expect(
        old.triggers.filter(
          (t: { function: string }) => t.function === "t.touch",
        ),
      ).toHaveLength(1);
      expect(old.class.relacl).toHaveLength(4);
    });

    test("classic model: parent without a primary key, per-partition primary key", async () => {
      // Shape of a pgslice-converted table: a parent with no primary key and a
      // non-unique partitioned index.
      await pool.query(sql.unsafe`
      CREATE TABLE t.sessions (
        id bigint NOT NULL,
        user_id bigint,
        created_at timestamptz NOT NULL
      ) PARTITION BY RANGE (created_at)
    `);
      await pool.query(sql.unsafe`CREATE INDEX ON t.sessions (user_id)`);
      await grantToRoles("sessions");
      await pool.query(
        sql.unsafe`COMMENT ON TABLE t.sessions IS 'column:created_at,period:month,cast:timestamptz,version:3'`,
      );

      await pool.query(sql.unsafe`
      CREATE TABLE t.sessions_202601 PARTITION OF t.sessions
      FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00')
    `);
      await pool.query(
        sql.unsafe`ALTER TABLE t.sessions_202601 ADD PRIMARY KEY (id)`,
      );
      await grantToRoles("sessions_202601");

      expect(await addPartitions("t.sessions")).toEqual(["sessions_202602"]);

      const old = await partitionSnapshot(pool, "sessions_202601");
      const attached = await partitionSnapshot(pool, "sessions_202602");
      expect(attached).toEqual(old);
      expect(
        old.indexes.map((i: { indisprimary: boolean }) => i.indisprimary),
      ).toContain(true);
    });
  },
);

describe("tablespace", { timeout: 30_000 }, () => {
  test("accepts an explicit tablespace", async () => {
    await createPosts();
    expect(
      await addPartitions("t.posts", { tablespace: "pg_default" }),
    ).toEqual(["posts_202602"]);
  });

  // Needs a tablespace that exists on the server, e.g. one created with
  // `CREATE TABLESPACE ... LOCATION '<dir>'` in the test container.
  const tablespace = process.env.PGSLICE_TEST_TABLESPACE;
  test.skipIf(!tablespace)(
    "places the new partition in the requested tablespace",
    async () => {
      await createPosts();
      await addPartitions("t.posts", { tablespace });
      expect(
        await pool.oneFirst(
          sql.type(z.object({ spcname: z.string().nullable() }))`
            SELECT spcname FROM pg_class c
            LEFT JOIN pg_tablespace s ON s.oid = c.reltablespace
            WHERE c.oid = 't.posts_202602'::regclass
          `,
        ),
      ).toBe(tablespace);
    },
  );
});

describe("end to end", { timeout: 60_000 }, () => {
  test("prep, add_partitions, fill, analyze, swap, status, maintain and unswap", async () => {
    await pool.query(sql.unsafe`
      CREATE TABLE t.visits (
        id bigserial PRIMARY KEY,
        path text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now()
      )
    `);
    await pool.query(
      sql.unsafe`GRANT SELECT ON t.visits TO ${sql.identifier([READER])}`,
    );
    await pool.query(sql.unsafe`
      INSERT INTO t.visits (path, created_at)
      SELECT 'p' || g, now() - (g || ' days')::interval
      FROM generate_series(0, 20) g
    `);

    await pgslice.start((c) =>
      pgslice.prep(c, {
        table: "t.visits",
        column: "created_at",
        period: "month",
        partition: true,
      }),
    );
    const created = await pgslice.start((c) =>
      pgslice.addPartitions(c, {
        table: "t.visits",
        intermediate: true,
        past: 1,
        future: 1,
      }),
    );
    expect(created).toHaveLength(3);
    expect(await partitionsOf("t.visits_intermediate")).toHaveLength(3);

    await pgslice.start(async (c) => {
      for await (const _batch of pgslice.fill(c, { table: "t.visits" })) {
        // Drain every batch.
      }
    });
    expect(
      await pool.oneFirst(
        sql.type(z.object({ count: z.number() }))`
          SELECT count(*)::int AS count FROM t.visits_intermediate
        `,
      ),
    ).toBe(21);

    await pgslice.analyze({ table: "t.visits" });
    await pgslice.start((c) => pgslice.swap(c, { table: "t.visits" }));

    const status = await pgslice.status({ table: "t.visits" });
    expect(status).toMatchObject({ partitionCount: 3 });

    const grantedToReader = await pool.anyFirst(
      sql.type(z.object({ name: z.string() }))`
        SELECT c.relname AS name FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        WHERE i.inhparent = 't.visits'::regclass
          AND has_table_privilege(${READER}, c.oid, 'SELECT')
      `,
    );
    expect(grantedToReader).toHaveLength(3);

    const results = await pgslice.start((c) =>
      pgslice.maintain(c, { schema: "t", futureMonthly: 3 }),
    );
    expect(results).toHaveLength(1);
    expect(results[0].error).toBeNull();
    expect(results[0].partitionsCreated).toHaveLength(2);
    expect(results[0].replicaIdentityReady).toBe(true);

    await pgslice.start((c) => pgslice.unswap(c, { table: "t.visits" }));
    expect(await partitionsOf("t.visits_intermediate")).toHaveLength(5);
  });
});
