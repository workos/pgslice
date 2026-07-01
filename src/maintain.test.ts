import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import {
  addChild,
  addDefault,
  addMinvalueChild,
  childNames,
  createCdcRole,
  DATE,
  nativeParent,
  pgsliceParent,
  TS,
  TSTZ,
} from "./testing/shapes.js";

/** A captured maintain JSONL log record, for asserting record shape from tests. */
interface MaintainLogRecord {
  msg: string;
  level: string;
  success?: number;
  target: { host?: string; db: string; schema?: string; table?: string };
}

/**
 * End-to-end validation of `maintain` against the real production table shapes
 * on PostgreSQL 13.20: catalog discovery, the native (parent-owned PK) vs
 * classic-pgslice (per-partition PK) distinction, grant inheritance, the
 * replica-identity behavior verified on PG 13 (the partitioned replica-identity
 * index propagates to new partitions automatically; the per-leaf identity falls
 * back to the composite key), idempotency, and the CDC-readiness guard.
 */
describe("Pgslice.maintain (fleet)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // 2026-01-15 is a Thursday; its ISO week is 2026-W03 (Monday 2026-01-12).
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("the full production fleet on one database", () => {
    test.beforeEach(async ({ transaction }) => {
      // 7 native tables.
      for (const posts of ["posts", "comments", "reactions"]) {
        await nativeParent(
          transaction,
          posts,
          "created_at",
          TS,
          "month",
          "date",
        );
        await addChild(
          transaction,
          posts,
          `${posts}_y2026m01`,
          "2026-01-01",
          "2026-02-01",
        );
      }

      await nativeParent(
        transaction,
        "daily_reports",
        "created_date",
        DATE,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "daily_reports",
        "daily_reports_202601",
        "2026-01-01",
        "2026-02-01",
      );

      await nativeParent(
        transaction,
        "analytics.visits",
        "created_at",
        TSTZ,
        "week",
        "timestamptz",
      );
      await addChild(
        transaction,
        "analytics.visits",
        "visits_2026w03",
        "2026-01-12 00:00:00+00",
        "2026-01-19 00:00:00+00",
      );
      await addDefault(transaction, "analytics.visits", "visits_default");

      await nativeParent(
        transaction,
        "audit_log",
        "occurred_at",
        TS,
        "week",
        "date",
      );
      await addChild(
        transaction,
        "audit_log",
        "audit_log_y2026w02",
        "2026-01-05",
        "2026-01-12",
      );

      await nativeParent(
        transaction,
        "outbox",
        "created_at",
        TSTZ,
        "week",
        "timestamptz",
      );
      // MINVALUE historic partition, ignored when anchoring.
      await addMinvalueChild(
        transaction,
        "outbox",
        "outbox_historic",
        "2025-01-01 00:00:00+00",
      );
      await addChild(
        transaction,
        "outbox",
        "outbox_p2026w03",
        "2026-01-12 00:00:00+00",
        "2026-01-19 00:00:00+00",
      );

      // 3 classic-pgslice tables (parent has no PK; each partition owns its own).
      for (const sessions of ["sessions", "tokens", "messages"]) {
        await pgsliceParent(
          transaction,
          sessions,
          "created_at",
          TSTZ,
          "week",
          "timestamptz",
        );
        await addChild(
          transaction,
          sessions,
          `${sessions}_2026w03`,
          "2026-01-12 00:00:00+00",
          "2026-01-19 00:00:00+00",
          ["id", "created_at"],
        );
      }

      // Noise that discovery must ignore: a partitioned parent with no settings
      // comment, and an ordinary (non-partitioned) table.
      await transaction.query(sql.unsafe`
        CREATE TABLE unmanaged_part (
          id bigint NOT NULL,
          created_at timestamp with time zone NOT NULL,
          PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
      `);
      await addChild(
        transaction,
        "unmanaged_part",
        "unmanaged_part_2026m01",
        "2026-01-01 00:00:00+00",
        "2026-02-01 00:00:00+00",
      );
      await transaction.query(
        sql.unsafe`CREATE TABLE regular_table (id bigint PRIMARY KEY)`,
      );
    });

    test("discovers exactly the settings-commented partitioned tables", async ({
      pgslice,
      transaction,
    }) => {
      const results = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });

      expect(results.map((r) => r.table).sort()).toEqual(
        [
          "public.outbox",
          "public.daily_reports",
          "public.tokens",
          "public.audit_log",
          "public.reactions",
          "public.comments",
          "public.posts",
          "public.sessions",
          "public.messages",
          "analytics.visits",
        ].sort(),
      );
    });

    test("classifies native vs pgslice tables correctly", async ({
      pgslice,
      transaction,
    }) => {
      const results = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });
      const model = new Map(results.map((r) => [r.table, r.model]));

      for (const native of [
        "public.posts",
        "public.comments",
        "public.reactions",
        "public.daily_reports",
        "analytics.visits",
        "public.audit_log",
        "public.outbox",
      ]) {
        expect(model.get(native)).toBe("native");
      }
      for (const classic of [
        "public.sessions",
        "public.tokens",
        "public.messages",
      ]) {
        expect(model.get(classic)).toBe("pgslice");
      }
    });

    test("extends every table by bounds with the expected pgslice-named partitions", async ({
      pgslice,
      transaction,
    }) => {
      const results = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });
      const created = new Map(
        results.map((r) => [r.table, [...r.partitionsCreated].sort()]),
      );

      expect(created.get("public.posts")).toEqual([
        "posts_202602",
        "posts_202603",
        "posts_202604",
      ]);
      expect(created.get("public.daily_reports")).toEqual([
        "daily_reports_202602",
        "daily_reports_202603",
        "daily_reports_202604",
      ]);
      expect(created.get("analytics.visits")).toEqual([
        "visits_2026w04",
        "visits_2026w05",
        "visits_2026w06",
      ]);
      expect(created.get("public.audit_log")).toEqual([
        "audit_log_2026w03",
        "audit_log_2026w04",
        "audit_log_2026w05",
        "audit_log_2026w06",
      ]);
      expect(created.get("public.outbox")).toEqual([
        "outbox_2026w04",
        "outbox_2026w05",
        "outbox_2026w06",
      ]);
      expect(created.get("public.sessions")).toEqual([
        "sessions_2026w04",
        "sessions_2026w05",
        "sessions_2026w06",
      ]);

      // Legacy-named and special partitions are left in place.
      expect(await childNames(transaction, "outbox")).toContain(
        "outbox_historic",
      );
      expect(await childNames(transaction, "analytics.visits")).toContain(
        "visits_default",
      );
      expect(await childNames(transaction, "public.posts")).toContain(
        "posts_y2026m01",
      );
    });

    test("every managed table ends up CDC-ready", async ({
      pgslice,
      transaction,
    }) => {
      const results = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });
      for (const result of results) {
        expect(result.replicaIdentityReady).toBe(true);
        expect(result.unsafePartitions).toEqual([]);
      }
    });

    test("is idempotent: a second run creates nothing", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });
      const second = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
      });
      for (const result of second) {
        expect(result.partitionsCreated).toEqual([]);
      }
    });

    test("can be restricted to a single schema", async ({
      pgslice,
      transaction,
    }) => {
      const results = await pgslice.maintain(transaction, {
        futureDaily: 3,
        futureWeekly: 3,
        futureMonthly: 3,
        futureYearly: 3,
        schema: "analytics",
      });
      expect(results.map((r) => r.table)).toEqual(["analytics.visits"]);
    });
  });

  describe("native tables", () => {
    test("inherit the parent composite PK and the parent's grants on new partitions", async ({
      pgslice,
      transaction,
    }) => {
      await createCdcRole(transaction, "replica_reader_maint");
      await nativeParent(
        transaction,
        "posts",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "posts",
        "posts_y2026m01",
        "2026-01-01",
        "2026-02-01",
      );
      await transaction.query(
        sql.unsafe`GRANT SELECT ON TABLE posts TO replica_reader_maint`,
      );

      await pgslice.maintain(transaction, {
        futureDaily: 1,
        futureWeekly: 1,
        futureMonthly: 1,
        futureYearly: 1,
      });

      const pkDefs = await transaction.any(
        sql.type(z.object({ def: z.string() }))`
          SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
          WHERE conrelid = 'public.posts_202602'::regclass AND contype = 'p'
        `,
      );
      expect(pkDefs.map((c) => c.def)).toEqual([
        "PRIMARY KEY (id, created_at)",
      ]);

      const grants = await transaction.any(
        sql.type(z.object({ grantee: z.string(), privilege: z.string() }))`
          SELECT grantee_role.rolname AS grantee, acl.privilege_type AS privilege
          FROM pg_class c
            CROSS JOIN LATERAL aclexplode(c.relacl) AS acl
            JOIN pg_roles grantee_role ON grantee_role.oid = acl.grantee
          WHERE c.oid = 'public.posts_202602'::regclass
            AND grantee_role.rolname = 'replica_reader_maint'
        `,
      );
      expect(grants.map((g) => g.privilege)).toContain("SELECT");
    });
  });

  describe("classic pgslice tables", () => {
    test("get a per-partition composite primary key", async ({
      pgslice,
      transaction,
    }) => {
      await pgsliceParent(
        transaction,
        "sessions",
        "created_at",
        TSTZ,
        "week",
        "timestamptz",
      );
      await addChild(
        transaction,
        "sessions",
        "sessions_2026w03",
        "2026-01-12 00:00:00+00",
        "2026-01-19 00:00:00+00",
        ["id", "created_at"],
      );

      const results = await pgslice.maintain(transaction, {
        futureDaily: 1,
        futureWeekly: 1,
        futureMonthly: 1,
        futureYearly: 1,
      });
      expect(results[0].model).toBe("pgslice");

      const pkDefs = await transaction.any(
        sql.type(z.object({ def: z.string() }))`
          SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
          WHERE conrelid = 'public.sessions_2026w04'::regclass AND contype = 'p'
        `,
      );
      expect(pkDefs.map((c) => c.def)).toEqual([
        "PRIMARY KEY (id, created_at)",
      ]);
    });
  });

  describe("replica identity", () => {
    test("a parent REPLICA IDENTITY USING INDEX propagates its partitioned index to new partitions automatically", async ({
      pgslice,
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "analytics.visits",
        "created_at",
        TSTZ,
        "week",
        "timestamptz",
      );
      await addChild(
        transaction,
        "analytics.visits",
        "visits_2026w03",
        "2026-01-12 00:00:00+00",
        "2026-01-19 00:00:00+00",
      );
      await addDefault(transaction, "analytics.visits", "visits_default");
      // Build the partitioned replica-identity index exactly as the runner does:
      // ON ONLY the parent, then attach a child index for every existing leaf,
      // then point REPLICA IDENTITY at it.
      await transaction.query(sql.unsafe`
        CREATE UNIQUE INDEX visits_ri_uidx
        ON ONLY analytics.visits (id, created_at)
      `);
      await transaction.query(sql.unsafe`
        CREATE UNIQUE INDEX visits_2026w03_ri
        ON analytics.visits_2026w03 (id, created_at)
      `);
      await transaction.query(sql.unsafe`
        ALTER INDEX analytics.visits_ri_uidx
        ATTACH PARTITION analytics.visits_2026w03_ri
      `);
      await transaction.query(sql.unsafe`
        CREATE UNIQUE INDEX visits_default_ri
        ON analytics.visits_default (id, created_at)
      `);
      await transaction.query(sql.unsafe`
        ALTER INDEX analytics.visits_ri_uidx
        ATTACH PARTITION analytics.visits_default_ri
      `);
      await transaction.query(sql.unsafe`
        ALTER TABLE analytics.visits
        REPLICA IDENTITY USING INDEX visits_ri_uidx
      `);

      const results = await pgslice.maintain(transaction, {
        futureDaily: 1,
        futureWeekly: 1,
        futureMonthly: 1,
        futureYearly: 1,
      });
      expect(results[0].replicaIdentityReady).toBe(true);

      // The new partition auto-got a unique (id, created_at) index attached to
      // the parent's partitioned replica-identity index, and it is valid. (The
      // new partition also has its own inherited composite PK index — that is a
      // second unique index, which is why we filter to the one attached to the
      // replica-identity index here.)
      const idx = await transaction.any(
        sql.type(z.object({ valid: z.boolean() }))`
          SELECT ix.indisvalid AS valid
          FROM pg_inherits pi
            JOIN pg_class parent_idx ON parent_idx.oid = pi.inhparent
            JOIN pg_class ic ON ic.oid = pi.inhrelid
            JOIN pg_index ix ON ix.indexrelid = ic.oid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
          WHERE parent_idx.relname = 'visits_ri_uidx'
            AND n.nspname = 'analytics'
            AND t.relname = 'visits_2026w04'
        `,
      );
      expect(idx).toHaveLength(1);
      expect(idx[0].valid).toBe(true);

      // The parent's replica-identity index stays valid after extension.
      const parentIdx = await transaction.one(
        sql.type(z.object({ valid: z.boolean() }))`
          SELECT ix.indisvalid AS valid FROM pg_index ix
            JOIN pg_class ic ON ic.oid = ix.indexrelid
          WHERE ic.relname = 'visits_ri_uidx'
        `,
      );
      expect(parentIdx.valid).toBe(true);
    });

    test("flags a pre-existing partition that has no usable replica identity", async ({
      pgslice,
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "weird",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "weird",
        "weird_2026m01",
        "2026-01-01",
        "2026-02-01",
      );
      // Simulate a leaf created outside pgslice that is not CDC-safe.
      await transaction.query(sql.unsafe`
        ALTER TABLE weird_2026m01 REPLICA IDENTITY NOTHING
      `);

      const results = await pgslice.maintain(transaction, {
        futureDaily: 1,
        futureWeekly: 1,
        futureMonthly: 1,
        futureYearly: 1,
      });
      const weird = results.find((r) => r.table === "public.weird");

      expect(weird?.replicaIdentityReady).toBe(false);
      expect(weird?.unsafePartitions).toEqual(["weird_2026m01"]);
    });
  });

  describe("failure isolation", () => {
    test("a non-empty DEFAULT blocking the next partition fails that table alone; the rest of the fleet still extends", async ({
      pgslice,
      transaction,
    }) => {
      // `blocked` has a DEFAULT holding a row that belongs in the next range,
      // so creating that partition will fail (the production drain footgun).
      await nativeParent(
        transaction,
        "blocked",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "blocked",
        "blocked_y2026m01",
        "2026-01-01",
        "2026-02-01",
      );
      await addDefault(transaction, "blocked", "blocked_default");
      await transaction.query(sql.unsafe`
        INSERT INTO blocked (id, created_at, payload) VALUES (1, '2026-02-15', 'absorbed')
      `);
      // `healthy` is an ordinary native monthly table.
      await nativeParent(
        transaction,
        "healthy",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "healthy",
        "healthy_y2026m01",
        "2026-01-01",
        "2026-02-01",
      );

      const results = await pgslice.maintain(transaction, {
        futureDaily: 1,
        futureWeekly: 1,
        futureMonthly: 1,
        futureYearly: 1,
      });
      const blocked = results.find((r) => r.table === "public.blocked");
      const healthy = results.find((r) => r.table === "public.healthy");

      // The blocked table's CREATE is rejected (slonik surfaces it as a check
      // integrity constraint violation); it is recorded as failed, not thrown.
      expect(blocked?.error).toBeTruthy();
      expect(blocked?.partitionsCreated).toEqual([]);
      // The healthy table is extended despite the other table's failure.
      expect(healthy?.error).toBeNull();
      expect(healthy?.partitionsCreated).toEqual(["healthy_202602"]);
    });

    test("emits an error record carrying the failure message and host", async ({
      pgslice,
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "blocked",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "blocked",
        "blocked_y2026m01",
        "2026-01-01",
        "2026-02-01",
      );
      await addDefault(transaction, "blocked", "blocked_default");
      await transaction.query(sql.unsafe`
        INSERT INTO blocked (id, created_at, payload) VALUES (1, '2026-02-15', 'absorbed')
      `);

      const lines: string[] = [];
      const results = await pgslice.maintain(
        transaction,
        { futureMonthly: 1, host: "clone.example" },
        (entry) => {
          lines.push(JSON.stringify(entry));
        },
      );
      const records = lines.map((line): MaintainLogRecord => JSON.parse(line));

      const blocked = results.find((r) => r.table === "public.blocked");
      const record = records.find((r) => r.target.table === "blocked");
      // The caught error is surfaced verbatim as the record's msg.
      expect(record?.msg).toBe(blocked?.error);
      expect(record?.level).toBe("error");
      expect(record?.success).toBe(0);
      expect(record?.target.host).toBe("clone.example");
    });
  });

  describe("per-period horizons", () => {
    test("extends each table by the horizon for its own period", async ({
      pgslice,
      transaction,
    }) => {
      // One run over a monthly and a weekly table with different per-period
      // futures. The weekly table (shorter period + larger future) must get
      // strictly more new partitions than the monthly one — proving the horizon
      // is applied per period, not uniformly.
      await nativeParent(
        transaction,
        "m_tbl",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "m_tbl",
        "m_tbl_y2026m01",
        "2026-01-01",
        "2026-02-01",
      );
      await nativeParent(
        transaction,
        "w_tbl",
        "created_at",
        TS,
        "week",
        "date",
      );
      await addChild(
        transaction,
        "w_tbl",
        "w_tbl_2026w02",
        "2026-01-05",
        "2026-01-12",
      );

      const results = await pgslice.maintain(transaction, {
        futureMonthly: 2,
        futureWeekly: 6,
      });
      const m = results.find((r) => r.table === "public.m_tbl");
      const w = results.find((r) => r.table === "public.w_tbl");

      expect(m?.error).toBeNull();
      expect(w?.error).toBeNull();
      const mCount = m?.partitionsCreated.length ?? 0;
      const wCount = w?.partitionsCreated.length ?? 0;
      expect(mCount).toBeGreaterThan(0);
      expect(wCount).toBeGreaterThan(mCount);
    });
  });
});
