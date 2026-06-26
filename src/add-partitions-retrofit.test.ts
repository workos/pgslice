import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql, type DatabaseTransactionConnection } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { assertContiguous } from "./testing/shapes.js";

/**
 * These tests cover retrofitting pgslice management onto tables that were
 * created outside of pgslice: a partitioned parent that already owns a
 * (composite) primary key, weekly partitioning, and copying the parent's
 * grants onto new partitions.
 */
describe("Pgslice.addPartitions (retrofit)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // 2026-01-15 is a Thursday; its ISO week is 2026-W03 (Monday 2026-01-12).
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  async function createCompositePkParent(
    transaction: DatabaseTransactionConnection,
    period: "month" | "week",
  ) {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id varchar NOT NULL,
        author_id varchar NOT NULL,
        created_at timestamp without time zone NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    await transaction.query(sql.unsafe`
      CREATE INDEX ON posts (author_id)
    `);
    await transaction.query(
      sql.unsafe`
        COMMENT ON TABLE posts IS ${sql.literalValue(
          `column:created_at,period:${period},cast:date,version:3`,
        )}
      `,
    );
  }

  describe("parent owns a composite primary key", () => {
    test.beforeEach(async ({ transaction }) => {
      await createCompositePkParent(transaction, "month");
    });

    test("creates future partitions without colliding on the inherited primary key", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, {
        table: "posts",
        future: 2,
      });

      const partitions = await transaction.any(
        sql.type(z.object({ tablename: z.string() }))`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename LIKE 'posts_2%'
          ORDER BY tablename
        `,
      );

      expect(partitions.map((p) => p.tablename)).toEqual([
        "posts_202601",
        "posts_202602",
        "posts_202603",
      ]);
    });

    test("new partitions inherit the parent composite key and gain no duplicate PK", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, { table: "posts", future: 1 });

      const constraints = await transaction.any(
        sql.type(z.object({ def: z.string() }))`
          SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
          WHERE conrelid = 'public.posts_202602'::regclass AND contype = 'p'
        `,
      );

      expect(constraints.map((c) => c.def)).toEqual([
        "PRIMARY KEY (id, created_at)",
      ]);
    });
  });

  describe("grant inheritance", () => {
    test.beforeEach(async ({ transaction }) => {
      await createCompositePkParent(transaction, "month");
      await transaction.query(sql.unsafe`
        DO $$ BEGIN
          IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgslice_grant_test') THEN
            CREATE ROLE pgslice_grant_test;
          END IF;
        END $$;
      `);
      await transaction.query(sql.unsafe`
        GRANT SELECT ON posts TO pgslice_grant_test
      `);
    });

    test("copies the parent's grants onto new partitions by default", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, { table: "posts", future: 1 });

      const { granted } = await transaction.one(
        sql.type(z.object({ granted: z.boolean() }))`
          SELECT has_table_privilege('pgslice_grant_test', 'public.posts_202602', 'SELECT') AS granted
        `,
      );

      expect(granted).toBe(true);
    });

    test("skips grant inheritance when inheritGrants is false", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, {
        table: "posts",
        future: 1,
        inheritGrants: false,
      });

      const { granted } = await transaction.one(
        sql.type(z.object({ granted: z.boolean() }))`
          SELECT has_table_privilege('pgslice_grant_test', 'public.posts_202602', 'SELECT') AS granted
        `,
      );

      expect(granted).toBe(false);
    });
  });

  describe("weekly period", () => {
    test.beforeEach(async ({ transaction }) => {
      await createCompositePkParent(transaction, "week");
    });

    test("creates ISO-week partitions with seven-day, Monday-aligned bounds", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, { table: "posts", future: 2 });

      const partitions = await transaction.any(
        sql.type(z.object({ name: z.string(), bound: z.string() }))`
          SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound
          FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
          WHERE i.inhparent = 'public.posts'::regclass
          ORDER BY c.relname
        `,
      );

      expect(partitions).toEqual([
        {
          name: "posts_2026w03",
          bound:
            "FOR VALUES FROM ('2026-01-12 00:00:00') TO ('2026-01-19 00:00:00')",
        },
        {
          name: "posts_2026w04",
          bound:
            "FOR VALUES FROM ('2026-01-19 00:00:00') TO ('2026-01-26 00:00:00')",
        },
        {
          name: "posts_2026w05",
          bound:
            "FOR VALUES FROM ('2026-01-26 00:00:00') TO ('2026-02-02 00:00:00')",
        },
      ]);
    });
  });

  describe("extending an existing partitioned table (by bounds, no rename)", () => {
    async function boundsByName(transaction: DatabaseTransactionConnection) {
      await transaction.query(sql.unsafe`SET LOCAL TIME ZONE 'UTC'`);
      return transaction.any(
        sql.type(z.object({ name: z.string(), bound: z.string() }))`
          SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound
          FROM pg_inherits i
            JOIN pg_class c ON c.oid = i.inhrelid
          WHERE i.inhparent = 'public.evt'::regclass
          ORDER BY c.relname
        `,
      );
    }

    test("extends a legacy-named weekly table in place, never renaming existing partitions", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:week,cast:date,version:3'
      `);
      // Legacy ISO-weekly partitions with a non-pgslice "p" name prefix.
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_p2026w24 PARTITION OF evt
          FOR VALUES FROM ('2026-06-08') TO ('2026-06-15')
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_p2026w25 PARTITION OF evt
          FOR VALUES FROM ('2026-06-15') TO ('2026-06-22')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 2 });

      // Legacy names are untouched; new partitions pick up at the bound.
      expect(await boundsByName(transaction)).toEqual([
        {
          name: "evt_2026w26",
          bound:
            "FOR VALUES FROM ('2026-06-22 00:00:00') TO ('2026-06-29 00:00:00')",
        },
        {
          name: "evt_2026w27",
          bound:
            "FOR VALUES FROM ('2026-06-29 00:00:00') TO ('2026-07-06 00:00:00')",
        },
        {
          name: "evt_p2026w24",
          bound:
            "FOR VALUES FROM ('2026-06-08 00:00:00') TO ('2026-06-15 00:00:00')",
        },
        {
          name: "evt_p2026w25",
          bound:
            "FOR VALUES FROM ('2026-06-15 00:00:00') TO ('2026-06-22 00:00:00')",
        },
      ]);
    });

    test("extends a year-resetting weekly scheme across the year boundary with no gap or overlap", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 11, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          occurred_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, occurred_at)
        ) PARTITION BY RANGE (occurred_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:occurred_at,period:week,cast:date,version:3'
      `);
      // Year-resetting weeks: w53 is a one-day stub ending exactly at the year
      // boundary. ISO-week generation would overlap it; bounds-anchored
      // extension continues contiguously from 2027-01-01.
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_y2026w52 PARTITION OF evt
          FOR VALUES FROM ('2026-12-24') TO ('2026-12-31')
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_y2026w53 PARTITION OF evt
          FOR VALUES FROM ('2026-12-31') TO ('2027-01-01')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 4 });

      const rows = await boundsByName(transaction);
      const firstGenerated = rows.find((r) => !r.name.startsWith("evt_y"));
      expect(firstGenerated?.bound).toBe(
        "FOR VALUES FROM ('2027-01-01 00:00:00') TO ('2027-01-08 00:00:00')",
      );

      // No two partitions overlap, and coverage is contiguous. (boundsByName
      // already pinned the session to UTC before reading the bounds.)
      assertContiguous(rows);
    });

    test("ignores a MINVALUE (historic) partition when choosing the extension anchor", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 10)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp with time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:week,cast:timestamptz,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_historic PARTITION OF evt
          FOR VALUES FROM (MINVALUE) TO ('2026-04-20 00:00:00+00')
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_p2026w24 PARTITION OF evt
          FOR VALUES FROM ('2026-06-08 00:00:00+00') TO ('2026-06-15 00:00:00+00')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 1 });

      // Nothing is created below the historic partition; extension anchors on
      // the latest finite upper bound (2026-06-15), not the MINVALUE partition.
      const generated = (await boundsByName(transaction)).filter(
        (r) => r.name !== "evt_historic" && r.name !== "evt_p2026w24",
      );
      expect(generated).toEqual([
        {
          name: "evt_2026w25",
          bound:
            "FOR VALUES FROM ('2026-06-15 00:00:00+00') TO ('2026-06-22 00:00:00+00')",
        },
      ]);
    });

    test("tolerates an existing DEFAULT partition and leaves it in place", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:week,cast:date,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_p2026w24 PARTITION OF evt
          FOR VALUES FROM ('2026-06-08') TO ('2026-06-15')
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_default PARTITION OF evt DEFAULT
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 1 });

      const rows = await boundsByName(transaction);
      expect(rows.find((r) => r.name === "evt_default")?.bound).toBe("DEFAULT");
      expect(rows.some((r) => r.name === "evt_2026w25")).toBe(true);
    });

    test("is idempotent: a second run creates no further partitions", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:month,cast:date,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_202606 PARTITION OF evt
          FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 3 });
      const afterFirst = (await boundsByName(transaction)).length;
      await pgslice.addPartitions(transaction, { table: "evt", future: 3 });
      const afterSecond = (await boundsByName(transaction)).length;

      expect(afterFirst).toBe(4);
      expect(afterSecond).toBe(afterFirst);
    });

    test("aligns new partitions to UTC regardless of the session timezone", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      // Deliberately operate under a non-UTC session timezone.
      await transaction.query(
        sql.unsafe`SET LOCAL TIME ZONE 'America/New_York'`,
      );
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp with time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:week,cast:timestamptz,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_p2026w25 PARTITION OF evt
          FOR VALUES FROM ('2026-06-15 00:00:00+00') TO ('2026-06-22 00:00:00+00')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 1 });

      // Boundary is the UTC midnight, contiguous with the existing partition —
      // not shifted by the session's -04:00 offset.
      const generated = (await boundsByName(transaction)).find(
        (r) => r.name === "evt_2026w26",
      );
      expect(generated?.bound).toBe(
        "FOR VALUES FROM ('2026-06-22 00:00:00+00') TO ('2026-06-29 00:00:00+00')",
      );
    });

    test("creates the current period at --future 0 when it is not yet covered", async ({
      pgslice,
      transaction,
    }) => {
      // today is June 2026; the latest existing partition is May, so today's
      // period is uncovered. --future 0 must still create June, matching the
      // fresh-table path which always includes the current period.
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:month,cast:date,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_202605 PARTITION OF evt
          FOR VALUES FROM ('2026-05-01') TO ('2026-06-01')
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 0 });

      const names = (await boundsByName(transaction)).map((r) => r.name);
      expect(names).toContain("evt_202606");
    });

    test("does not extend forward past a MAXVALUE catch-all partition", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:month,cast:date,version:3'
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_202606 PARTITION OF evt
          FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')
      `);
      // An open-ended catch-all above already covers every future row.
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_future PARTITION OF evt
          FOR VALUES FROM ('2026-07-01') TO (MAXVALUE)
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 3 });

      // Forward extension is disabled when a partition is unbounded above —
      // there is nothing to add, and no error is raised.
      expect(
        (await boundsByName(transaction)).map((r) => r.name).sort(),
      ).toEqual(["evt_202606", "evt_future"]);
    });

    test("extends a classic (parent has no PK) table with the inherited composite key", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 5, 15)));
      await transaction.query(sql.unsafe`
        CREATE TABLE evt (
          id varchar NOT NULL,
          tenant_id varchar NOT NULL,
          created_at timestamp without time zone NOT NULL
        ) PARTITION BY RANGE (created_at)
      `);
      await transaction.query(sql.unsafe`
        COMMENT ON TABLE evt IS 'column:created_at,period:month,cast:date,version:3'
      `);
      // Classic pgslice model: the parent has no key; each partition owns a
      // (here three-column) composite key of its own.
      await transaction.query(sql.unsafe`
        CREATE TABLE evt_202606 PARTITION OF evt
          FOR VALUES FROM ('2026-06-01') TO ('2026-07-01')
      `);
      await transaction.query(sql.unsafe`
        ALTER TABLE evt_202606 ADD PRIMARY KEY (tenant_id, id, created_at)
      `);

      await pgslice.addPartitions(transaction, { table: "evt", future: 1 });

      const pk = await transaction.any(
        sql.type(z.object({ def: z.string() }))`
          SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
          WHERE conrelid = 'public.evt_202607'::regclass AND contype = 'p'
        `,
      );
      expect(pk.map((c) => c.def)).toEqual([
        "PRIMARY KEY (tenant_id, id, created_at)",
      ]);
    });
  });
});
