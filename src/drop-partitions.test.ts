import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql, type DatabaseTransactionConnection } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { DropPartitionsError } from "./pgslice.js";
import { Table } from "./table.js";
import {
  addChild,
  addDefault,
  addMinvalueChild,
  childNames,
  ident,
  nativeParent,
  pgsliceParent,
  TS,
  TSTZ,
} from "./testing/shapes.js";
import type { Cast, Period } from "./types.js";

/** Writes a settings comment that carries a retention. */
async function setRetention(
  tx: DatabaseTransactionConnection,
  qualified: string,
  column: string,
  period: Period,
  cast: Cast,
  retention: string,
): Promise<void> {
  await tx.query(
    sql.unsafe`COMMENT ON TABLE ${ident(qualified)} IS ${sql.literalValue(
      `column:${column},period:${period},cast:${cast},version:3,retention:${retention}`,
    )}`,
  );
}

/** A native monthly table with partitions for 2025-09 .. 2026-02. */
async function monthlyPosts(
  tx: DatabaseTransactionConnection,
  retention = "2",
): Promise<void> {
  await nativeParent(tx, "posts", "created_at", TS, "month", "date");
  await setRetention(tx, "posts", "created_at", "month", "date", retention);
  const months = [
    ["202509", "2025-09-01", "2025-10-01"],
    ["202510", "2025-10-01", "2025-11-01"],
    ["202511", "2025-11-01", "2025-12-01"],
    ["202512", "2025-12-01", "2026-01-01"],
    ["202601", "2026-01-01", "2026-02-01"],
    ["202602", "2026-02-01", "2026-03-01"],
  ];
  for (const [suffix, from, to] of months) {
    await addChild(tx, "posts", `posts_${suffix}`, from, to);
  }
}

describe("Pgslice.dropPartitions", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    // 2026-01-15 is a Thursday; its ISO week is 2026-W03 (Monday 2026-01-12).
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test("drops partitions older than the retention window, oldest first", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);
    await transaction.query(
      sql.unsafe`INSERT INTO posts VALUES (1, '2025-09-10', 'old'), (2, '2026-01-10', 'new')`,
    );

    // retention:2 keeps January (current) plus November and December.
    const dropped = await pgslice.dropPartitions(transaction, {
      table: "posts",
    });

    expect(dropped).toEqual(["posts_202509", "posts_202510"]);
    expect(await childNames(transaction, "posts")).toEqual([
      "posts_202511",
      "posts_202512",
      "posts_202601",
      "posts_202602",
    ]);
    const { count } = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT count(*) FROM posts
      `,
    );
    expect(Number(count)).toBe(1);
  });

  test("is idempotent", async ({ pgslice, transaction }) => {
    await monthlyPosts(transaction);

    await pgslice.dropPartitions(transaction, { table: "posts" });
    const second = await pgslice.dropPartitions(transaction, {
      table: "posts",
    });

    expect(second).toEqual([]);
  });

  test("dryRun reports the expired partitions without dropping them", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);

    const wouldDrop = await pgslice.dropPartitions(transaction, {
      table: "posts",
      dryRun: true,
    });

    expect(wouldDrop).toEqual(["posts_202509", "posts_202510"]);
    expect(await childNames(transaction, "posts")).toHaveLength(6);
  });

  test("never drops the current or future partitions, even with retention:1", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction, "1");

    await pgslice.dropPartitions(transaction, { table: "posts" });

    expect(await childNames(transaction, "posts")).toEqual([
      "posts_202512",
      "posts_202601",
      "posts_202602",
    ]);
  });

  test("refuses a table without a retention setting", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(transaction, "posts", "created_at", TS, "month", "date");
    await addChild(
      transaction,
      "posts",
      "posts_202001",
      "2020-01-01",
      "2020-02-01",
    );

    await expect(
      pgslice.dropPartitions(transaction, { table: "posts" }),
    ).rejects.toThrow("No retention configured: public.posts");
    expect(await childNames(transaction, "posts")).toEqual(["posts_202001"]);
  });

  test("never drops DEFAULT or MINVALUE partitions", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(transaction, "posts", "created_at", TS, "month", "date");
    await setRetention(
      transaction,
      "posts",
      "created_at",
      "month",
      "date",
      "1",
    );
    await addMinvalueChild(
      transaction,
      "posts",
      "posts_historic",
      "2025-01-01",
    );
    await addChild(
      transaction,
      "posts",
      "posts_202501",
      "2025-01-01",
      "2025-02-01",
    );
    await addChild(
      transaction,
      "posts",
      "posts_202601",
      "2026-01-01",
      "2026-02-01",
    );
    await addDefault(transaction, "posts", "posts_default");

    const dropped = await pgslice.dropPartitions(transaction, {
      table: "posts",
    });

    expect(dropped).toEqual(["posts_202501"]);
    expect(await childNames(transaction, "posts")).toEqual([
      "posts_202601",
      "posts_default",
      "posts_historic",
    ]);
  });

  test("never drops a sub-partitioned child", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(transaction, "posts", "created_at", TS, "month", "date");
    await setRetention(
      transaction,
      "posts",
      "created_at",
      "month",
      "date",
      "1",
    );
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_202501 PARTITION OF posts
      FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')
      PARTITION BY RANGE (created_at)
    `);

    const dropped = await pgslice.dropPartitions(transaction, {
      table: "posts",
    });

    expect(dropped).toEqual([]);
    expect(await childNames(transaction, "posts")).toEqual(["posts_202501"]);
  });

  test("selects by bounds, not names, for legacy-named partitions", async ({
    pgslice,
    transaction,
  }) => {
    // A year-resetting weekly scheme: a one-day stub closes out 2025.
    await nativeParent(
      transaction,
      "activities",
      "occurred_at",
      TS,
      "week",
      "date",
    );
    await setRetention(
      transaction,
      "activities",
      "occurred_at",
      "week",
      "date",
      "2",
    );
    const weeks = [
      ["activities_p2025w52", "2025-12-24", "2025-12-31"],
      ["activities_p2025w53", "2025-12-31", "2026-01-01"],
      ["activities_p2026w01", "2026-01-01", "2026-01-08"],
      ["activities_p2026w02", "2026-01-08", "2026-01-15"],
      ["activities_p2026w03", "2026-01-15", "2026-01-22"],
    ];
    for (const [name, from, to] of weeks) {
      await addChild(transaction, "activities", name, from, to);
    }

    // The current ISO week starts 2026-01-12, so retention:2 cuts off at
    // 2025-12-29: only partitions ending on or before then are dropped.
    const dropped = await pgslice.dropPartitions(transaction, {
      table: "activities",
    });

    expect(dropped).toEqual([]);

    vi.setSystemTime(new Date(Date.UTC(2026, 0, 22)));
    // Cutoff 2026-01-05: the 2025w52 week and the w53 stub both end before it.
    expect(
      await pgslice.dropPartitions(transaction, { table: "activities" }),
    ).toEqual(["activities_p2025w52", "activities_p2025w53"]);
  });

  test("compares timestamptz bounds in UTC regardless of the session time zone", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(
      transaction,
      "events",
      "created_at",
      TSTZ,
      "month",
      "timestamptz",
    );
    await setRetention(
      transaction,
      "events",
      "created_at",
      "month",
      "timestamptz",
      "1",
    );
    await transaction.query(sql.unsafe`SET LOCAL TIME ZONE 'UTC'`);
    await addChild(
      transaction,
      "events",
      "events_202511",
      "2025-11-01",
      "2025-12-01",
    );
    await addChild(
      transaction,
      "events",
      "events_202512",
      "2025-12-01",
      "2026-01-01",
    );
    // Under a +14:00 session the bounds render shifted (e.g. November's upper
    // bound reads as 2025-12-01 14:00), so without pinning UTC the partition
    // ending exactly at the cutoff would be missed.
    await transaction.query(
      sql.unsafe`SET LOCAL TIME ZONE 'Pacific/Kiritimati'`,
    );

    const dropped = await pgslice.dropPartitions(transaction, {
      table: "events",
    });

    expect(dropped).toEqual(["events_202511"]);
  });

  test("drops classic pgslice partitions and those in another schema", async ({
    pgslice,
    transaction,
  }) => {
    await pgsliceParent(
      transaction,
      "app.visits",
      "created_at",
      TS,
      "month",
      "date",
    );
    await setRetention(
      transaction,
      "app.visits",
      "created_at",
      "month",
      "date",
      "1",
    );
    await addChild(
      transaction,
      "app.visits",
      "visits_202510",
      "2025-10-01",
      "2025-11-01",
      ["id"],
    );
    await transaction.query(sql.unsafe`CREATE SCHEMA IF NOT EXISTS archive`);
    await transaction.query(sql.unsafe`
      CREATE TABLE archive.visits_202511 PARTITION OF app.visits
      FOR VALUES FROM ('2025-11-01') TO ('2025-12-01')
    `);

    const dropped = await pgslice.dropPartitions(transaction, {
      table: "app.visits",
    });

    expect(dropped).toEqual(["visits_202510", "visits_202511"]);
    const leftover = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT count(*) FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname IN ('app', 'archive') AND c.relname LIKE 'visits_2025%'
      `,
    );
    expect(Number(leftover.count)).toBe(0);
  });

  test("keeps every surviving leaf CDC-safe", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);

    await pgslice.dropPartitions(transaction, { table: "posts" });

    expect(
      await new Table("public", "posts").unsafeReplicaIdentityPartitions(
        transaction,
      ),
    ).toEqual([]);
  });

  test("does not cascade: a dependent object stops the run and survives", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);
    await transaction.query(
      sql.unsafe`CREATE VIEW october_posts AS SELECT * FROM posts_202510`,
    );

    const error = await pgslice
      .dropPartitions(transaction, { table: "posts" })
      .catch((e: unknown) => e);

    expect(error).toBeInstanceOf(DropPartitionsError);
    expect(error instanceof DropPartitionsError && error.dropped).toEqual([
      "posts_202509",
    ]);
    expect(String(error)).toContain("public.posts_202510");
    expect(await childNames(transaction, "posts")).toContain("posts_202510");
    const view = await transaction.maybeOne(
      sql.type(z.object({ exists: z.boolean() }))`
        SELECT to_regclass('october_posts') IS NOT NULL AS exists
      `,
    );
    expect(view?.exists).toBe(true);
  });

  test("defaults to a 5s lock_timeout for each drop", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);

    await pgslice.dropPartitions(transaction, { table: "posts" });

    // Each drop runs in a savepoint whose released SET LOCAL propagates to this
    // transaction, so reading it here proves the DROP ran under the timeout.
    const result = await transaction.one(
      sql.type(z.object({ lock_timeout: z.string() }))`SHOW lock_timeout`,
    );
    expect(result.lock_timeout).toBe("5s");
  });

  test("honors a custom lockTimeout", async ({ pgslice, transaction }) => {
    await monthlyPosts(transaction);

    await pgslice.dropPartitions(transaction, {
      table: "posts",
      lockTimeout: "750ms",
    });

    const result = await transaction.one(
      sql.type(z.object({ lock_timeout: z.string() }))`SHOW lock_timeout`,
    );
    expect(result.lock_timeout).toBe("750ms");
  });
});

describe("Pgslice.maintain with dropExpired", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  const horizons = {
    futureDaily: 1,
    futureWeekly: 1,
    futureMonthly: 2,
    futureYearly: 1,
  };

  test("does not drop anything unless dropExpired is set", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);

    const [result] = await pgslice.maintain(transaction, horizons);

    expect(result.partitionsDropped).toEqual([]);
    expect(await childNames(transaction, "posts")).toContain("posts_202509");
  });

  test("extends and prunes tables with a retention, leaving others alone", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);
    await nativeParent(
      transaction,
      "comments",
      "created_at",
      TS,
      "month",
      "date",
    );
    await addChild(
      transaction,
      "comments",
      "comments_202001",
      "2020-01-01",
      "2026-02-01",
    );

    const results = await pgslice.maintain(transaction, {
      ...horizons,
      dropExpired: true,
    });

    const posts = results.find((r) => r.table === "public.posts");
    expect(posts?.error).toBeNull();
    expect(posts?.partitionsCreated).toEqual(["posts_202603"]);
    expect(posts?.partitionsDropped).toEqual(["posts_202509", "posts_202510"]);
    expect(posts?.replicaIdentityReady).toBe(true);

    const comments = results.find((r) => r.table === "public.comments");
    expect(comments?.error).toBeNull();
    expect(comments?.partitionsDropped).toEqual([]);
    expect(await childNames(transaction, "comments")).toContain(
      "comments_202001",
    );
  });

  test("records a failed drop without losing the extension or the fleet", async ({
    pgslice,
    transaction,
  }) => {
    await monthlyPosts(transaction);
    await transaction.query(
      sql.unsafe`CREATE VIEW october_posts AS SELECT * FROM posts_202510`,
    );
    await nativeParent(
      transaction,
      "reactions",
      "created_at",
      TS,
      "month",
      "date",
    );
    await setRetention(
      transaction,
      "reactions",
      "created_at",
      "month",
      "date",
      "1",
    );
    await addChild(
      transaction,
      "reactions",
      "reactions_202511",
      "2025-11-01",
      "2025-12-01",
    );
    await addChild(
      transaction,
      "reactions",
      "reactions_202601",
      "2026-01-01",
      "2026-02-01",
    );

    const logs: Array<Record<string, unknown>> = [];
    const results = await pgslice.maintain(
      transaction,
      { ...horizons, dropExpired: true },
      (entry) => logs.push(entry),
    );

    const posts = results.find((r) => r.table === "public.posts");
    expect(posts?.error).toContain("public.posts_202510");
    expect(posts?.partitionsCreated).toEqual(["posts_202603"]);
    expect(posts?.partitionsDropped).toEqual(["posts_202509"]);

    const reactions = results.find((r) => r.table === "public.reactions");
    expect(reactions?.error).toBeNull();
    expect(reactions?.partitionsDropped).toEqual(["reactions_202511"]);

    const postsLog = logs.find(
      (entry) =>
        typeof entry.target === "object" &&
        entry.target !== null &&
        "table" in entry.target &&
        entry.target.table === "posts",
    );
    expect(postsLog?.level).toBe("error");
    expect(postsLog?.partitions).toEqual({ new: 1, dropped: 1, total: 0 });
    expect(postsLog?.droppedPartitions).toEqual(["posts_202509"]);
  });
});
