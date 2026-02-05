import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { Filler } from "./filler.js";

describe("Filler", () => {
  describe("with numeric IDs", () => {
    test("fills data in batches", async ({ transaction }) => {
      // Create source and destination tables with proper naming
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name)
        SELECT 'item_' || i FROM generate_series(1, 25) AS i
      `);

      const filler = await Filler.init(transaction, {
        table: "posts",
        batchSize: 10,
      });

      const batches: Array<{ batchNumber: number }> = [];
      for await (const { batchNumber } of filler.fill(transaction)) {
        batches.push({ batchNumber });
      }

      expect(batches).toEqual([
        { batchNumber: 1 },
        { batchNumber: 2 },
        { batchNumber: 3 },
      ]);

      // Verify data was copied
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM posts_intermediate
        `,
      );
      expect(count.count).toBe(25);
    });

    test("respects start option", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')
      `);

      // Start from 2, inclusive (should include id=2)
      const filler = await Filler.init(transaction, {
        table: "posts",
        batchSize: 10,
        start: "2",
      });

      for await (const _batch of filler.fill(transaction)) {
        // consume batches
      }

      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM posts_intermediate
        `,
      );
      expect(count.count).toBe(4); // IDs 2, 3, 4, 5
    });

    test("handles empty source table", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);

      const filler = await Filler.init(transaction, {
        table: "posts",
        batchSize: 10,
      });

      const batches = [];
      for await (const batch of filler.fill(transaction)) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(0);
    });
  });

  describe("with ULID IDs", () => {
    test("fills data in batches", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id TEXT PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id TEXT PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (id, name) VALUES
          ('01ARZ3NDEKTSV4RRFFQ69G5FAA', 'a'),
          ('01ARZ3NDEKTSV4RRFFQ69G5FAB', 'b'),
          ('01ARZ3NDEKTSV4RRFFQ69G5FAC', 'c'),
          ('01ARZ3NDEKTSV4RRFFQ69G5FAD', 'd'),
          ('01ARZ3NDEKTSV4RRFFQ69G5FAE', 'e')
      `);

      const filler = await Filler.init(transaction, {
        table: "posts",
        batchSize: 2,
      });

      const batches: Array<{ batchNumber: number }> = [];
      for await (const { batchNumber } of filler.fill(transaction)) {
        batches.push({ batchNumber });
      }

      // ULID batches don't know total count (same as numeric now)
      expect(batches.length).toBeGreaterThanOrEqual(2);

      // Verify data was copied
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM posts_intermediate
        `,
      );
      expect(count.count).toBe(5);
    });
  });

  describe("with ON CONFLICT DO NOTHING", () => {
    test("handles duplicate keys gracefully", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);
      // Pre-populate dest with some data
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'existing'), (2, 'existing')
      `);

      const filler = await Filler.init(transaction, {
        table: "posts",
        batchSize: 10,
      });

      for await (const _batch of filler.fill(transaction)) {
        // consume batches
      }

      // Should have 3 rows (2 existing + 1 new, duplicates ignored)
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM posts_intermediate
        `,
      );
      expect(count.count).toBe(3);

      // Existing data should be unchanged
      const existing = await transaction.one(
        sql.type(z.object({ name: z.string() }))`
          SELECT name FROM posts_intermediate WHERE id = 1
        `,
      );
      expect(existing.name).toBe("existing");
    });
  });
});

describe("Pgslice.fill", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  test("fills unpartitioned table", async ({ pgslice, transaction }) => {
    // Create source table with data
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO posts (title)
      SELECT 'Post ' || i FROM generate_series(1, 15) AS i
    `);

    // Create intermediate table
    await pgslice.prep(transaction, {
      table: "posts",
      partition: false,
    });

    // Fill data
    const batches = [];
    for await (const batch of pgslice.fill(transaction, { table: "posts", batchSize: 10 })) {
      batches.push(batch);
    }

    expect(batches.length).toBe(2);

    // Verify data was copied
    const count = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(count.count).toBe(15);
  });

  test("fills partitioned table", async ({ pgslice, transaction }) => {
    // Create source table with data
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id BIGSERIAL PRIMARY KEY,
        created_at DATE NOT NULL,
        title TEXT
      )
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO posts (created_at, title) VALUES
        ('2026-01-15', 'Post 1'),
        ('2026-01-15', 'Post 2'),
        ('2026-01-15', 'Post 3')
    `);

    // Create partitioned intermediate table
    await pgslice.prep(transaction, {
      table: "posts",
      column: "created_at",
      period: "month",
      partition: true,
    });

    // Add partition for January 2026
    await pgslice.addPartitions(transaction, {
      table: "posts",
      intermediate: true,
      past: 0,
      future: 0,
    });

    // Fill data
    const batches = [];
    for await (const batch of pgslice.fill(transaction, { table: "posts", batchSize: 10 })) {
      batches.push(batch);
    }

    expect(batches.length).toBe(1);

    // Verify data was copied
    const count = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(count.count).toBe(3);
  });

  test("returns nothing to fill for empty source", async ({
    pgslice,
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT)
    `);

    await pgslice.prep(transaction, {
      table: "posts",
      partition: false,
    });

    const batches = [];
    for await (const batch of pgslice.fill(transaction, { table: "posts" })) {
      batches.push(batch);
    }

    expect(batches).toHaveLength(0);
  });

  test("respects --start option", async ({ pgslice, transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO posts (title)
      SELECT 'Post ' || i FROM generate_series(1, 10) AS i
    `);

    await pgslice.prep(transaction, {
      table: "posts",
      partition: false,
    });

    // Start from ID 5
    const batches = [];
    for await (const batch of pgslice.fill(transaction, {
      table: "posts",
      start: "5",
      batchSize: 100,
    })) {
      batches.push(batch);
    }

    expect(batches.length).toBe(1);

    // Should have copied IDs 5-10 (6 rows)
    const count = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(count.count).toBe(6);
  });

  test("throws for missing source table", async ({ pgslice, transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, title TEXT)
    `);

    const error = await (async () => {
      for await (const _batch of pgslice.fill(transaction, { table: "posts" })) {
        // should not reach here
      }
    })().catch((e) => e);

    expect(error.message).toBe("Table not found: public.posts");
  });

  test("throws for missing destination table", async ({
    pgslice,
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT)
    `);

    const error = await (async () => {
      for await (const _batch of pgslice.fill(transaction, { table: "posts" })) {
        // should not reach here
      }
    })().catch((e) => e);

    expect(error.message).toBe("Table not found: public.posts_intermediate");
  });

  test("throws for table without primary key", async ({
    pgslice,
    transaction,
  }) => {
    // Tables without `id` column won't match the fallback
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (post_id BIGSERIAL, title TEXT)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_intermediate (post_id BIGSERIAL, title TEXT)
    `);

    const error = await (async () => {
      for await (const _batch of pgslice.fill(transaction, { table: "posts" })) {
        // should not reach here
      }
    })().catch((e) => e);

    expect(error.message).toBe('Primary key not found in "public.posts".');
  });
});
