import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { Synchronizer } from "./synchronizer.js";

describe("Synchronizer", () => {
  describe("init", () => {
    test("throws when source table does not exist", async ({ transaction }) => {
      // Only create the intermediate table
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe("Table not found: public.posts");
    });

    test("throws when target table does not exist", async ({ transaction }) => {
      // Only create the source table
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe("Table not found: public.posts_intermediate");
    });

    test("throws when source has column not in target", async ({
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, extra_col TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe(
        "Column 'extra_col' exists in public.posts but not in public.posts_intermediate",
      );
    });

    test("throws when target has column not in source", async ({
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, extra_col TEXT)
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe(
        "Column 'extra_col' exists in public.posts_intermediate but not in public.posts",
      );
    });

    test("throws when source table is empty", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe("No rows found in source table");
    });

    test("throws when primary key not found and not specified", async ({
      transaction,
    }) => {
      // Create tables without primary key
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('test')
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
      }).catch((e) => e);

      expect(error.message).toBe(
        "Primary key not found. Specify with --primary-key",
      );
    });

    test("throws when specified primary key not found in source", async ({
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('test')
      `);

      const error = await Synchronizer.init(transaction, {
        table: "posts",
        primaryKey: "nonexistent_column",
      }).catch((e) => e);

      expect(error.message).toBe(
        "Primary key 'nonexistent_column' not found in source table",
      );
    });
  });

  describe("synchronize", () => {
    test("detects and inserts missing rows", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);
      // Target is empty - all rows are missing

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          rowsInserted: 3,
          rowsUpdated: 0,
          rowsDeleted: 0,
          matchingRows: 0,
        }),
      ]);

      // Verify rows were inserted
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
      );
      expect(count.count).toBe(3);
    });

    test("detects and updates different rows", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('updated_a'), ('updated_b')
      `);
      // Target has same IDs but different names
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'old_a'), (2, 'old_b')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          rowsInserted: 0,
          rowsUpdated: 2,
          rowsDeleted: 0,
        }),
      ]);

      // Verify rows were updated
      const rows = await transaction.any(
        sql.type(z.object({ id: z.coerce.bigint(), name: z.string() }))`
        SELECT id, name FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, name: "updated_a" },
        { id: 2n, name: "updated_b" },
      ]);
    });

    test("detects and deletes extra rows", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGINT PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGINT PRIMARY KEY, name TEXT)
      `);
      // Source has ids 1 and 3 (gap at 2)
      await transaction.query(sql.unsafe`
        INSERT INTO posts (id, name) VALUES (1, 'a'), (3, 'c')
      `);
      // Target has ids 1, 2, 3 - id=2 is extra (within the range 1-3)
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'a'), (2, 'extra'), (3, 'c')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          rowsDeleted: 1,
          matchingRows: 2,
        }),
      ]);

      // Verify extra row was deleted
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
      );
      expect(count.count).toBe(2);
    });

    test("processes in batches", async ({ transaction }) => {
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

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
        windowSize: 10,
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // 25 rows with windowSize=10 should produce 3 batches
      expect(batches.map((b) => b.batchNumber)).toEqual([1, 2, 3]);

      // Total rows inserted should be 25
      const totalInserted = batches.reduce((sum, b) => sum + b.rowsInserted, 0);
      expect(totalInserted).toBe(25);
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

      // Start from id=3
      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
        start: "3",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // Should have synchronized 3 rows (ids 3, 4, 5)
      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);
    });

    test("reports matching rows", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);
      // Copy same data to target
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 3,
          rowsInserted: 0,
          rowsUpdated: 0,
          rowsDeleted: 0,
        }),
      ]);
    });

    test("dry-run mode skips mutations", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);
      // Target is empty

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
        dryRun: true,
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // Batch should report would-be changes
      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // But no actual changes should have been made
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
      );
      expect(count.count).toBe(0);
    });

    test("handles mixed operations in single batch", async ({
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      // Source: id=1 (matching), id=2 (needs update), id=3 (missing)
      await transaction.query(sql.unsafe`
        INSERT INTO posts (id, name) VALUES (1, 'same'), (2, 'updated'), (3, 'new')
      `);
      // Target: id=1 (matching), id=2 (different), id=4 (extra)
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'same'), (2, 'old'), (4, 'extra')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // Note: id=4 is NOT deleted because it's outside the batch range (1-3)
      // The synchronize only checks within the primary key range of the source batch
      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 1, // id=1
          rowsUpdated: 1, // id=2
          rowsInserted: 1, // id=3
          rowsDeleted: 0,
        }),
      ]);

      // Verify final state
      const rows = await transaction.any(
        sql.type(z.object({ id: z.coerce.number(), name: z.string() }))`
        SELECT id, name FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toHaveLength(4); // id=1,2,3 from sync + id=4 still there
    });

    test("handles ULID primary keys", async ({ transaction }) => {
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
          ('01ARZ3NDEKTSV4RRFFQ69G5FAC', 'c')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify rows were inserted
      const count = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
      );
      expect(count.count).toBe(3);
    });

    test("reports batch duration", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(1);
      expect(batches[0].batchDurationMs).toBeGreaterThanOrEqual(0);
    });

    test("reports primary key range", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          primaryKeyRange: { start: 1n, end: 3n },
        }),
      ]);
    });

    test("handles NULL values in columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, description TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, description TEXT)
      `);
      // Source has NULL values
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, description) VALUES ('a', NULL), ('b', 'has description'), (NULL, 'c')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify NULL values were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            name: z.string().nullable(),
            description: z.string().nullable(),
          }),
        )`
        SELECT id, name, description FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, name: "a", description: null },
        { id: 2n, name: "b", description: "has description" },
        { id: 3n, name: null, description: "c" },
      ]);
    });

    test("handles timestamp columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, created_at) VALUES
          ('a', '2024-01-15 10:30:00+00'),
          ('b', '2024-06-20 14:45:30+00'),
          ('c', NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify timestamps were preserved by comparing source and target
      const sourceRows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            created_at: z.number().nullable(),
          }),
        )`
        SELECT id, EXTRACT(EPOCH FROM created_at)::bigint as created_at
        FROM posts ORDER BY id
      `,
      );
      const targetRows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            created_at: z.number().nullable(),
          }),
        )`
        SELECT id, EXTRACT(EPOCH FROM created_at)::bigint as created_at
        FROM posts_intermediate ORDER BY id
      `,
      );

      // Timestamps should match between source and target
      expect(targetRows).toEqual(sourceRows);
    });

    test("detects differences in timestamp columns", async ({
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, created_at TIMESTAMPTZ)
      `);
      // Source has updated timestamps
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, created_at) VALUES
          ('a', '2024-01-15 10:30:00+00'),
          ('b', '2024-06-20 14:45:30+00')
      `);
      // Target has old timestamps
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name, created_at) VALUES
          (1, 'a', '2024-01-15 10:30:00+00'),
          (2, 'b', '2023-01-01 00:00:00+00')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 1, // id=1 matches
          rowsUpdated: 1, // id=2 has different timestamp
        }),
      ]);
    });

    test("handles bigint columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, view_count BIGINT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, view_count BIGINT)
      `);
      // Use large numbers that exceed JavaScript's safe integer range
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, view_count) VALUES
          ('a', 9007199254740993),
          ('b', 9007199254740994),
          ('c', NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify bigint values were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.number(),
            view_count: z.coerce.bigint().nullable(),
          }),
        )`
        SELECT id, view_count FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, view_count: 9007199254740993n },
        { id: 2n, view_count: 9007199254740994n },
        { id: 3n, view_count: null },
      ]);
    });

    test("detects differences in bigint columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, view_count BIGINT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, view_count BIGINT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, view_count) VALUES
          ('a', 9007199254740993),
          ('b', 100)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name, view_count) VALUES
          (1, 'a', 9007199254740993),
          (2, 'b', 200)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 1, // id=1 matches (same bigint)
          rowsUpdated: 1, // id=2 has different view_count
        }),
      ]);
    });

    test("handles updates with NULL values", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT, description TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT, description TEXT)
      `);
      // Source has NULL where target has value, and value where target has NULL
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, description) VALUES ('a', NULL), ('b', 'now has description')
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name, description) VALUES
          (1, 'a', 'had description'),
          (2, 'b', NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsUpdated: 2 })]);

      // Verify NULL handling in updates
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.number(),
            description: z.string().nullable(),
          }),
        )`
        SELECT id, description FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, description: null }, // Was 'had description', now NULL
        { id: 2n, description: "now has description" }, // Was NULL, now has value
      ]);
    });

    test("handles mixed column types", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id BIGSERIAL PRIMARY KEY,
          name TEXT,
          view_count BIGINT,
          rating NUMERIC(3,2),
          is_published BOOLEAN,
          created_at TIMESTAMPTZ
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id BIGSERIAL PRIMARY KEY,
          name TEXT,
          view_count BIGINT,
          rating NUMERIC(3,2),
          is_published BOOLEAN,
          created_at TIMESTAMPTZ
        )
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name, view_count, rating, is_published, created_at) VALUES
          ('post1', 1000, 4.5, true, '2024-01-15 10:30:00+00'),
          ('post2', NULL, NULL, false, NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 2 })]);

      // Verify all types were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            name: z.string(),
            view_count: z.coerce.bigint().nullable(),
            rating: z.string().nullable(), // NUMERIC comes back as string
            is_published: z.boolean(),
          }),
        )`
        SELECT id, name, view_count, rating::text, is_published
        FROM posts_intermediate ORDER BY id
      `,
      );

      expect(rows).toEqual([
        expect.objectContaining({
          name: "post1",
          view_count: 1000n,
          rating: "4.50",
          is_published: true,
        }),
        expect.objectContaining({
          name: "post2",
          view_count: null,
          rating: null,
          is_published: false,
        }),
      ]);

      // Verify timestamps match between source and target
      const sourceTs = await transaction.one(
        sql.type(z.object({ ts: z.number().nullable() }))`
        SELECT EXTRACT(EPOCH FROM created_at)::bigint as ts FROM posts WHERE id = 1
      `,
      );
      const targetTs = await transaction.one(
        sql.type(z.object({ ts: z.number().nullable() }))`
        SELECT EXTRACT(EPOCH FROM created_at)::bigint as ts FROM posts_intermediate WHERE id = 1
      `,
      );
      expect(targetTs.ts).toBe(sourceTs.ts);
    });

    test("handles large BIGINT values in timestamp-like range correctly", async ({
      transaction,
    }) => {
      // This tests that BIGINT values in the range that could be mistaken for timestamps
      // (500_000_000_000 to 10_000_000_000_000) are treated as plain integers, not timestamps
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, large_value BIGINT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, large_value BIGINT)
      `);
      // Use values in the "timestamp-like" range
      await transaction.query(sql.unsafe`
        INSERT INTO posts (large_value) VALUES
          (600000000000),
          (1000000000000),
          (5000000000000)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify values were preserved as BIGINT, not converted to timestamps
      const rows = await transaction.any(
        sql.type(
          z.object({ id: z.coerce.bigint(), large_value: z.coerce.bigint() }),
        )`
        SELECT id, large_value FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, large_value: 600000000000n },
        { id: 2n, large_value: 1000000000000n },
        { id: 3n, large_value: 5000000000000n },
      ]);
    });

    test("handles UUID columns", async ({ transaction }) => {
      // Use BIGSERIAL as PK since PostgreSQL doesn't support MIN/MAX on UUID
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, uuid_col UUID)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, uuid_col UUID)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (uuid_col) VALUES
          ('550e8400-e29b-41d4-a716-446655440000'),
          ('6ba7b810-9dad-11d1-80b4-00c04fd430c8'),
          (NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify UUIDs were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({ id: z.coerce.bigint(), uuid_col: z.string().nullable() }),
        )`
        SELECT id, uuid_col::text FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, uuid_col: "550e8400-e29b-41d4-a716-446655440000" },
        { id: 2n, uuid_col: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" },
        { id: 3n, uuid_col: null },
      ]);
    });

    test("handles DATE columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, event_date DATE)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, event_date DATE)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (event_date) VALUES
          ('2024-01-15'),
          ('2024-06-20'),
          (NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify dates were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            event_date: z.string().nullable(),
          }),
        )`
        SELECT id, event_date::text FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, event_date: "2024-01-15" },
        { id: 2n, event_date: "2024-06-20" },
        { id: 3n, event_date: null },
      ]);
    });

    test("handles BYTEA columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, data BYTEA)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, data BYTEA)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (data) VALUES
          (E'\\x48656c6c6f'),
          (E'\\x576f726c64'),
          (NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify bytea values match between source and target
      const sourceRows = await transaction.any(
        sql.type(
          z.object({ id: z.coerce.bigint(), data: z.string().nullable() }),
        )`
        SELECT id, encode(data, 'hex') as data FROM posts ORDER BY id
      `,
      );
      const targetRows = await transaction.any(
        sql.type(
          z.object({ id: z.coerce.bigint(), data: z.string().nullable() }),
        )`
        SELECT id, encode(data, 'hex') as data FROM posts_intermediate ORDER BY id
      `,
      );

      expect(targetRows).toEqual(sourceRows);
    });

    test("handles JSONB columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (metadata) VALUES
          ('{"key": "value", "number": 42}'),
          ('["a", "b", "c"]'),
          (NULL)
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([expect.objectContaining({ rowsInserted: 3 })]);

      // Verify JSONB values were preserved
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            metadata: z.unknown().nullable(),
          }),
        )`
        SELECT id, metadata FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, metadata: { key: "value", number: 42 } },
        { id: 2n, metadata: ["a", "b", "c"] },
        { id: 3n, metadata: null },
      ]);
    });

    test("detects matching JSONB values", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      // Source has JSONB data
      await transaction.query(sql.unsafe`
        INSERT INTO posts (metadata) VALUES
          ('{"key": "value", "number": 42}'),
          ('["a", "b", "c"]')
      `);
      // Target has identical JSONB data
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, metadata) VALUES
          (1, '{"key": "value", "number": 42}'),
          (2, '["a", "b", "c"]')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // Both rows should match - no updates needed
      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 2,
          rowsUpdated: 0,
          rowsInserted: 0,
          rowsDeleted: 0,
        }),
      ]);
    });

    test("detects differences in JSONB columns", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      // Source has updated JSONB data
      await transaction.query(sql.unsafe`
        INSERT INTO posts (metadata) VALUES
          ('{"key": "updated", "number": 100}'),
          ('["x", "y", "z"]')
      `);
      // Target has old JSONB data
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, metadata) VALUES
          (1, '{"key": "old", "number": 42}'),
          (2, '["a", "b", "c"]')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 0,
          rowsUpdated: 2,
        }),
      ]);

      // Verify rows were updated with new JSONB values
      const rows = await transaction.any(
        sql.type(
          z.object({
            id: z.coerce.bigint(),
            metadata: z.unknown(),
          }),
        )`
        SELECT id, metadata FROM posts_intermediate ORDER BY id
      `,
      );
      expect(rows).toEqual([
        { id: 1n, metadata: { key: "updated", number: 100 } },
        { id: 2n, metadata: ["x", "y", "z"] },
      ]);
    });

    test("handles complex nested JSONB", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, metadata JSONB)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (metadata) VALUES
          ('{"nested": {"deep": {"value": [1, 2, 3]}, "array": [{"a": 1}, {"b": 2}]}}')
      `);
      // Target has same nested structure
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, metadata) VALUES
          (1, '{"nested": {"deep": {"value": [1, 2, 3]}, "array": [{"a": 1}, {"b": 2}]}}')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // Complex nested JSONB should match
      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 1,
          rowsUpdated: 0,
        }),
      ]);
    });

    test("handles JSON columns (non-binary)", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, metadata JSON)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, metadata JSON)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (metadata) VALUES
          ('{"key": "value"}'),
          ('["item1", "item2"]')
      `);
      // Target has same JSON data
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, metadata) VALUES
          (1, '{"key": "value"}'),
          (2, '["item1", "item2"]')
      `);

      const synchronizer = await Synchronizer.init(transaction, {
        table: "posts",
      });

      const batches = [];
      for await (const batch of synchronizer.synchronize(transaction)) {
        batches.push(batch);
      }

      // JSON columns should also match correctly
      expect(batches).toEqual([
        expect.objectContaining({
          matchingRows: 2,
          rowsUpdated: 0,
        }),
      ]);
    });
  });
});
