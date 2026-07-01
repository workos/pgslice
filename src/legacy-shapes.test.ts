import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import {
  addChild,
  addDefault,
  addMinvalueChild,
  assertContiguous,
  childBounds,
  childNames,
  createCdcRole,
  DATE,
  nativeParent,
  TS,
  TSTZ,
} from "./testing/shapes.js";

/**
 * End-to-end coverage of the retrofit engine against the full range of
 * real-world partitioned-table shapes, on PostgreSQL 13.20 (the production
 * version). Each shape is exercised through `add_partitions`. The shapes use
 * generic names; they stand in for the heterogeneous schemes a long-lived
 * application accumulates (monthly, ISO-week, and year-resetting-weekly periods;
 * `timestamp`/`timestamptz`/`date` keys; composite parent-owned keys; DEFAULT
 * and MINVALUE catch-alls; non-public schemas; replication grants).
 */
describe("legacy partitioned-table shapes", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  // The retrofit features exist because stock Postgres behavior on these shapes
  // is wrong or fatal. These tests pin that current behavior so the features
  // are justified rather than assumed.
  describe("stock Postgres behavior the retrofit must compensate for", () => {
    test("does not propagate a parent's grants to a newly-created partition", async ({
      transaction,
    }) => {
      await createCdcRole(transaction, "cdc_stock_grants");
      await nativeParent(
        transaction,
        "metrics",
        "created_at",
        TS,
        "month",
        "date",
      );
      await transaction.query(
        sql.unsafe`GRANT SELECT ON TABLE metrics TO cdc_stock_grants`,
      );

      // Create the partition directly, so pgslice's grant inheritance never runs.
      await addChild(
        transaction,
        "metrics",
        "metrics_202602",
        "2026-02-01",
        "2026-03-01",
      );

      const { granted } = await transaction.one(
        sql.type(z.object({ granted: z.boolean() }))`
          SELECT has_table_privilege('cdc_stock_grants', 'public.metrics_202602', 'SELECT') AS granted
        `,
      );
      // Postgres does NOT cascade the parent's SELECT — this is the recurring
      // "a replication role loses access to a new partition" failure this fixes.
      expect(granted).toBe(false);
    });

    test("rejects a per-child primary key when the parent already owns one", async ({
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "metrics",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "metrics",
        "metrics_202602",
        "2026-02-01",
        "2026-03-01",
      );

      // The child already carries the parent's propagated PK; adding another is
      // fatal — which is why the engine skips the per-child ADD PRIMARY KEY for
      // parent-owned-PK tables.
      await expect(
        transaction.query(sql.unsafe`
          ALTER TABLE metrics_202602 ADD PRIMARY KEY (id, created_at)
        `),
      ).rejects.toThrow(/multiple primary keys/i);
    });
  });

  describe("non-midnight partition boundaries (unsupported)", () => {
    test("rejects loudly instead of silently creating nothing", async ({
      pgslice,
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "skewed",
        "created_at",
        TSTZ,
        "month",
        "timestamptz",
      );
      // An existing partition whose bounds sit at 06:00 UTC, not midnight.
      await addChild(
        transaction,
        "skewed",
        "skewed_202602",
        "2026-02-01 06:00:00+00",
        "2026-03-01 06:00:00+00",
      );

      // New bounds are emitted at UTC midnight, so this shape can't be extended
      // contiguously; it must fail loudly rather than return [] ("no new
      // partitions needed") while forward coverage silently stalls.
      await expect(
        pgslice.addPartitions(transaction, { table: "skewed", future: 2 }),
      ).rejects.toThrow(/midnight/i);
    });
  });

  describe("monthly, date key, with a parent UNIQUE + CHECK", () => {
    test("extends and new partitions inherit the composite PK, UNIQUE, and CHECK", async ({
      pgslice,
      transaction,
    }) => {
      await nativeParent(
        transaction,
        "rollups",
        "bucket_date",
        DATE,
        "month",
        "date",
        {
          extraColumns: sql.fragment`,
        session_id bigint NOT NULL,
        kind text NOT NULL`,
          unique: ["session_id", "kind", "bucket_date"],
          check: sql.fragment`kind <> ''`,
        },
      );
      await addChild(
        transaction,
        "rollups",
        "rollups_202601",
        "2026-01-01",
        "2026-02-01",
      );

      const created = await pgslice.addPartitions(transaction, {
        table: "rollups",
        future: 2,
      });
      expect([...created].sort()).toEqual(["rollups_202602", "rollups_202603"]);

      // The new leaf inherits the composite PK and the CHECK, and the parent's
      // UNIQUE + CHECK don't block extension. (The PK and CHECK propagate as
      // child constraints; the UNIQUE propagates as an attached index.)
      const constraints = await transaction.any(
        sql.type(z.object({ def: z.string() }))`
          SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint
          WHERE conrelid = 'public.rollups_202602'::regclass
          ORDER BY def
        `,
      );
      const defs = constraints.map((c) => c.def);
      expect(defs).toContain("PRIMARY KEY (id, bucket_date)");
      expect(defs.some((d) => /CHECK .*kind/.test(d))).toBe(true);

      const uniqueIndexes = await transaction.any(
        sql.type(z.object({ indexdef: z.string() }))`
          SELECT indexdef FROM pg_indexes
          WHERE schemaname = 'public' AND tablename = 'rollups_202602'
            AND indexdef ILIKE '%UNIQUE%'
            AND indexdef ILIKE '%session_id%'
        `,
      );
      expect(uniqueIndexes.length).toBeGreaterThan(0);
    });
  });

  describe("schema-qualified ISO-week table with a DEFAULT and parent indexes", () => {
    test("new partitions inherit the full parent index set, incl. a partial index, and the empty DEFAULT is left in place", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 9, 20)));
      await nativeParent(
        transaction,
        "archive.events_weekly",
        "created_at",
        TS,
        "week",
        "date",
      );
      await addChild(
        transaction,
        "archive.events_weekly",
        "events_weekly_p2026w43",
        "2026-10-19",
        "2026-10-26",
      );
      await addDefault(
        transaction,
        "archive.events_weekly",
        "events_weekly_default",
      );
      // A secondary index and a partial index, declared on the parent so they
      // propagate to every existing and future leaf.
      await transaction.query(sql.unsafe`
        CREATE INDEX events_weekly_payload_idx ON archive.events_weekly (payload)
      `);
      await transaction.query(sql.unsafe`
        CREATE INDEX events_weekly_active_idx ON archive.events_weekly (id)
        WHERE payload IS NOT NULL
      `);

      await pgslice.addPartitions(transaction, {
        table: "archive.events_weekly",
        future: 1,
      });

      // The new leaf must carry the same index set (count + the partial) as a
      // pre-existing leaf — a missing-index regression would show up here.
      const indexCount = async (leaf: string) => {
        const { count } = await transaction.one(
          sql.type(z.object({ count: z.coerce.number() }))`
            SELECT count(*) FROM pg_indexes
            WHERE schemaname = 'archive' AND tablename = ${leaf}
          `,
        );
        return count;
      };
      const legacy = await indexCount("events_weekly_p2026w43");
      const fresh = await indexCount("events_weekly_2026w44");
      expect(fresh).toBe(legacy);

      const partials = await transaction.any(
        sql.type(z.object({ indexdef: z.string() }))`
          SELECT indexdef FROM pg_indexes
          WHERE schemaname = 'archive' AND tablename = 'events_weekly_2026w44'
            AND indexdef ILIKE '%WHERE%'
        `,
      );
      expect(partials.length).toBeGreaterThan(0);

      // The empty DEFAULT is tolerated and left untouched.
      expect(await childNames(transaction, "archive.events_weekly")).toContain(
        "events_weekly_default",
      );
    });
  });

  describe("timestamptz weekly across the ISO week-53 year boundary", () => {
    test("extends contiguously into the new ISO year with UTC-aligned bounds, ignoring the MINVALUE historic", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 11, 28)));
      await nativeParent(
        transaction,
        "outbox",
        "created_at",
        TSTZ,
        "week",
        "timestamptz",
      );
      await addMinvalueChild(
        transaction,
        "outbox",
        "outbox_historic",
        "2025-01-01 00:00:00+00",
      );
      // ISO week 53 of 2026 is the Monday-aligned 2026-12-28..2027-01-04.
      await addChild(
        transaction,
        "outbox",
        "outbox_p2026w53",
        "2026-12-28 00:00:00+00",
        "2027-01-04 00:00:00+00",
      );

      await pgslice.addPartitions(transaction, { table: "outbox", future: 2 });

      const generated = (await childBounds(transaction, "outbox")).filter(
        (r) => r.name !== "outbox_historic" && r.name !== "outbox_p2026w53",
      );
      // The first new partition is ISO 2027-W01, chained off the w53 upper
      // bound, UTC-aligned (+00), with no overlap onto the historic partition.
      expect(generated[0]).toEqual({
        name: "outbox_2027w01",
        bound:
          "FOR VALUES FROM ('2027-01-04 00:00:00+00') TO ('2027-01-11 00:00:00+00')",
      });
      assertContiguous(
        (await childBounds(transaction, "outbox")).filter(
          (r) => r.name !== "outbox_historic",
        ),
      );
    });
  });

  // The hard case: weeks numbered from Jan 1, so the weekday drifts year to year
  // (Wed in 2025) and there is a one-day stub at each year boundary. The engine
  // extends by bounds, so it continues the drifted scheme contiguously rather
  // than snapping to an absolute Monday-aligned ISO calendar (which would
  // overlap). The generated names are ISO `w<IW>` and may differ cosmetically
  // from the legacy `y<YYYY>w<WW>` names — names are non-functional.
  describe("year-resetting weekly with a drifted (non-Monday) weekday", () => {
    test("continues the drifted weekday by bounds; first new partition has the expected name and bounds", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2025, 10, 5)));
      await nativeParent(
        transaction,
        "ledger",
        "occurred_at",
        TS,
        "week",
        "date",
      );
      // Wednesday-aligned 2025 weeks, as a Jan-1-anchored scheme produces.
      await addChild(
        transaction,
        "ledger",
        "ledger_y2025w44",
        "2025-10-29",
        "2025-11-05",
      );
      await addChild(
        transaction,
        "ledger",
        "ledger_y2025w45",
        "2025-11-05",
        "2025-11-12",
      );

      await pgslice.addPartitions(transaction, { table: "ledger", future: 2 });

      const generated = (await childBounds(transaction, "ledger")).filter(
        (r) => !r.name.startsWith("ledger_y"),
      );
      // Chained off the 2025-11-12 (Wednesday) upper bound — still Wednesday,
      // NOT snapped to Monday. The ISO suffix names the week containing it.
      expect(generated[0]).toEqual({
        name: "ledger_2025w46",
        bound:
          "FOR VALUES FROM ('2025-11-12 00:00:00') TO ('2025-11-19 00:00:00')",
      });
      // Drift is preserved: the new partition starts on a Wednesday (day 3).
      expect(new Date("2025-11-12T00:00:00Z").getUTCDay()).toBe(3);
      // Legacy partitions are untouched and coverage stays contiguous.
      const names = await childNames(transaction, "ledger");
      expect(names).toContain("ledger_y2025w44");
      expect(names).toContain("ledger_y2025w45");
      assertContiguous(await childBounds(transaction, "ledger"));
    });

    test("crosses the year boundary off the one-day stub, with no rename and no name collision", async ({
      pgslice,
      transaction,
    }) => {
      vi.setSystemTime(new Date(Date.UTC(2026, 11, 28)));
      await nativeParent(
        transaction,
        "ledger",
        "occurred_at",
        TS,
        "week",
        "date",
      );
      // The year-end one-day stub the year-resetting scheme leaves at 2026-12-31.
      await addChild(
        transaction,
        "ledger",
        "ledger_y2026w52",
        "2026-12-24",
        "2026-12-31",
      );
      await addChild(
        transaction,
        "ledger",
        "ledger_y2026w53",
        "2026-12-31",
        "2027-01-01",
      );

      await pgslice.addPartitions(transaction, { table: "ledger", future: 1 });

      const generated = (await childBounds(transaction, "ledger")).filter(
        (r) => !r.name.startsWith("ledger_y"),
      );
      // Anchored on 2027-01-01 (a Friday); ISO 8601 places it in week 53 of
      // 2026, so the suffix reads `2026w53` even though it covers a 2027 week.
      // Cosmetically odd, but create-safe (distinct from the legacy
      // `ledger_y2026w53`) and contiguous.
      expect(generated[0]).toEqual({
        name: "ledger_2026w53",
        bound:
          "FOR VALUES FROM ('2027-01-01 00:00:00') TO ('2027-01-08 00:00:00')",
      });
      expect(await childNames(transaction, "ledger")).toContain(
        "ledger_y2026w53",
      );
      assertContiguous(await childBounds(transaction, "ledger"));
    });
  });

  describe("grant inheritance breadth", () => {
    test("re-issues multiple privileges, preserving WITH GRANT OPTION", async ({
      pgslice,
      transaction,
    }) => {
      await createCdcRole(transaction, "cdc_multi_priv");
      await nativeParent(
        transaction,
        "metrics",
        "created_at",
        TS,
        "month",
        "date",
      );
      await addChild(
        transaction,
        "metrics",
        "metrics_202601",
        "2026-01-01",
        "2026-02-01",
      );
      await transaction.query(sql.unsafe`
        GRANT SELECT ON TABLE metrics TO cdc_multi_priv WITH GRANT OPTION
      `);
      await transaction.query(sql.unsafe`
        GRANT INSERT, UPDATE ON TABLE metrics TO cdc_multi_priv
      `);

      await pgslice.addPartitions(transaction, { table: "metrics", future: 1 });

      const privs = await transaction.one(
        sql.type(
          z.object({
            sel: z.boolean(),
            sel_grant: z.boolean(),
            ins: z.boolean(),
            upd: z.boolean(),
          }),
        )`
          SELECT
            has_table_privilege('cdc_multi_priv', 'public.metrics_202602', 'SELECT') AS sel,
            has_table_privilege('cdc_multi_priv', 'public.metrics_202602', 'SELECT WITH GRANT OPTION') AS sel_grant,
            has_table_privilege('cdc_multi_priv', 'public.metrics_202602', 'INSERT') AS ins,
            has_table_privilege('cdc_multi_priv', 'public.metrics_202602', 'UPDATE') AS upd
        `,
      );
      expect(privs).toEqual({
        sel: true,
        sel_grant: true,
        ins: true,
        upd: true,
      });
    });
  });
});
