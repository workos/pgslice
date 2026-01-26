import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { Synchronizer } from "./synchronizer.js";

describe("Synchronizer.init", () => {
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

  test("throws when source has column not in target", async ({ transaction }) => {
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

  test("throws when target has column not in source", async ({ transaction }) => {
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

    expect(error.message).toBe("Primary key not found. Specify with --primary-key");
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

    expect(error.message).toBe("Primary key 'nonexistent_column' not found in source table");
  });
});

describe("Synchronizer.synchronize", () => {
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

    expect(batches).toHaveLength(1);
    expect(batches[0].rowsInserted).toBe(3);
    expect(batches[0].rowsUpdated).toBe(0);
    expect(batches[0].rowsDeleted).toBe(0);
    expect(batches[0].matchingRows).toBe(0);

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

    expect(batches).toHaveLength(1);
    expect(batches[0].rowsInserted).toBe(0);
    expect(batches[0].rowsUpdated).toBe(2);
    expect(batches[0].rowsDeleted).toBe(0);

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

    expect(batches).toHaveLength(1);
    expect(batches[0].rowsDeleted).toBe(1);
    expect(batches[0].matchingRows).toBe(2);

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
    expect(batches).toHaveLength(3);
    expect(batches[0].batchNumber).toBe(1);
    expect(batches[1].batchNumber).toBe(2);
    expect(batches[2].batchNumber).toBe(3);

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

    expect(batches).toHaveLength(1);
    // Should have synchronized 3 rows (ids 3, 4, 5)
    expect(batches[0].rowsInserted).toBe(3);
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

    expect(batches).toHaveLength(1);
    expect(batches[0].matchingRows).toBe(3);
    expect(batches[0].rowsInserted).toBe(0);
    expect(batches[0].rowsUpdated).toBe(0);
    expect(batches[0].rowsDeleted).toBe(0);
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

    expect(batches).toHaveLength(1);
    // Batch should report would-be changes
    expect(batches[0].rowsInserted).toBe(3);

    // But no actual changes should have been made
    const count = await transaction.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(count.count).toBe(0);
  });

  test("handles mixed operations in single batch", async ({ transaction }) => {
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

    expect(batches).toHaveLength(1);
    expect(batches[0].matchingRows).toBe(1); // id=1
    expect(batches[0].rowsUpdated).toBe(1); // id=2
    expect(batches[0].rowsInserted).toBe(1); // id=3
    // Note: id=4 is NOT deleted because it's outside the batch range (1-3)
    // The synchronize only checks within the primary key range of the source batch
    expect(batches[0].rowsDeleted).toBe(0);

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

    expect(batches).toHaveLength(1);
    expect(batches[0].rowsInserted).toBe(3);

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

    expect(batches).toHaveLength(1);
    expect(batches[0].primaryKeyRange.start).toBe(1n);
    expect(batches[0].primaryKeyRange.end).toBe(3n);
  });
});

