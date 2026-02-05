import { afterEach, beforeEach, describe, expect, vi } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";

describe("Pgslice.prep", () => {
  test.beforeEach(async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        author_id INTEGER NOT NULL,
        category_id INTEGER,
        created_at DATE NOT NULL
      )
    `);
  });

  describe("partitioned tables", () => {
    test("creates a partitioned intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ relkind: z.string() }))`
          SELECT relkind FROM pg_class
          WHERE relname = 'posts_intermediate'
        `,
      );
      expect(result.relkind).toBe("p");
    });

    test("copies indexes to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_title ON posts (title)
      `);

      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.any(
        sql.type(z.object({ indexname: z.string() }))`
          SELECT indexname FROM pg_indexes
          WHERE tablename = 'posts_intermediate' AND indexname LIKE '%title%'
        `,
      );
      expect(result).toEqual([{ indexname: "posts_intermediate_title_idx" }]);
    });

    test("copies multiple indexes to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_title ON posts (title)
      `);
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_author ON posts (author_id)
      `);

      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.any(
        sql.type(z.object({ indexname: z.string() }))`
          SELECT indexname FROM pg_indexes
          WHERE tablename = 'posts_intermediate'
        `,
      );
      expect(result).toEqual([
        { indexname: "posts_intermediate_title_idx" },
        { indexname: "posts_intermediate_author_id_idx" },
      ]);
    });

    test("copies foreign keys to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE authors (id SERIAL PRIMARY KEY)
      `);
      await transaction.query(sql.unsafe`
        ALTER TABLE posts ADD CONSTRAINT fk_author
        FOREIGN KEY (author_id) REFERENCES authors(id)
      `);

      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.any(
        sql.type(z.object({ conname: z.string() }))`
          SELECT conname FROM pg_constraint
          WHERE conrelid = 'public.posts_intermediate'::regclass AND contype = 'f'
        `,
      );
      expect(result).toEqual([
        { conname: "posts_intermediate_author_id_fkey" },
      ]);
    });

    test("copies multiple foreign keys to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE authors (id SERIAL PRIMARY KEY)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE categories (id SERIAL PRIMARY KEY)
      `);
      await transaction.query(sql.unsafe`
        ALTER TABLE posts ADD CONSTRAINT fk_author
        FOREIGN KEY (author_id) REFERENCES authors(id)
      `);
      await transaction.query(sql.unsafe`
        ALTER TABLE posts ADD CONSTRAINT fk_category
        FOREIGN KEY (category_id) REFERENCES categories(id)
      `);

      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.any(
        sql.type(z.object({ conname: z.string() }))`
          SELECT conname FROM pg_constraint
          WHERE conrelid = 'public.posts_intermediate'::regclass AND contype = 'f'
          ORDER BY conname
        `,
      );
      expect(result).toEqual([
        { conname: "posts_intermediate_author_id_fkey" },
        { conname: "posts_intermediate_category_id_fkey" },
      ]);
    });

    test("stores correct metadata in table comment", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ comment: z.string().nullable() }))`
          SELECT obj_description('public.posts_intermediate'::regclass) AS comment
        `,
      );
      expect(result.comment).toBe(
        "column:created_at,period:month,cast:date,version:3",
      );
    });
  });

  describe("non-partitioned tables", () => {
    test("creates a non-partitioned intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.prep(transaction, {
        table: "posts",
        partition: false,
      });

      const result = await transaction.one(
        sql.type(z.object({ relkind: z.string() }))`
          SELECT relkind FROM pg_class
          WHERE relname = 'posts_intermediate'
        `,
      );
      expect(result.relkind).toBe("r");
    });
  });

  describe("column type detection", () => {
    test("detects date column cast", async ({ pgslice, transaction }) => {
      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ comment: z.string().nullable() }))`
          SELECT obj_description('public.posts_intermediate'::regclass) AS comment
        `,
      );
      expect(result.comment).toContain("cast:date");
    });

    test("detects timestamptz column cast", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id SERIAL PRIMARY KEY,
          occurred_at TIMESTAMPTZ NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "events",
        column: "occurred_at",
        period: "day",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ comment: z.string().nullable() }))`
          SELECT obj_description('public.events_intermediate'::regclass) AS comment
        `,
      );
      expect(result.comment).toContain("cast:timestamptz");
    });
  });

  describe("period types", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id SERIAL PRIMARY KEY,
          occurred_at DATE NOT NULL
        )
      `);
    });

    test("stores correct metadata with day period", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.prep(transaction, {
        table: "events",
        column: "occurred_at",
        period: "day",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ comment: z.string().nullable() }))`
          SELECT obj_description('public.events_intermediate'::regclass) AS comment
        `,
      );
      expect(result.comment).toBe(
        "column:occurred_at,period:day,cast:date,version:3",
      );
    });

    test("stores correct metadata with year period", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.prep(transaction, {
        table: "events",
        column: "occurred_at",
        period: "year",
        partition: true,
      });

      const result = await transaction.one(
        sql.type(z.object({ comment: z.string().nullable() }))`
          SELECT obj_description('public.events_intermediate'::regclass) AS comment
        `,
      );
      expect(result.comment).toBe(
        "column:occurred_at,period:year,cast:date,version:3",
      );
    });
  });

  describe("schema handling", () => {
    test("handles schema-qualified table names", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`CREATE SCHEMA myschema`);
      await transaction.query(sql.unsafe`
        CREATE TABLE myschema.posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "myschema.posts",
        column: "created_at",
        period: "month",
        partition: true,
      });

      const result = await transaction.maybeOne(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*) FROM pg_tables
          WHERE schemaname = 'myschema' AND tablename = 'posts_intermediate'
        `,
      );
      expect(Number(result?.count)).toBe(1);
    });
  });

  describe("error handling", () => {
    test("throws when table not found", async ({ pgslice, transaction }) => {
      await expect(
        pgslice.prep(transaction, {
          table: "nonexistent",
          column: "created_at",
          period: "month",
          partition: true,
        }),
      ).rejects.toThrow("Table not found: public.nonexistent");
    });

    test("throws when column not found", async ({ pgslice, transaction }) => {
      await expect(
        pgslice.prep(transaction, {
          table: "posts",
          column: "nonexistent_column",
          period: "month",
          partition: true,
        }),
      ).rejects.toThrow("Column not found: nonexistent_column");
    });

    test("throws for invalid period", async ({ pgslice, transaction }) => {
      await expect(
        pgslice.prep(transaction, {
          table: "posts",
          column: "created_at",
          period: "invalid" as "month",
          partition: true,
        }),
      ).rejects.toThrow("Invalid period: invalid");
    });

    test("throws when intermediate table already exists", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id SERIAL PRIMARY KEY)
      `);

      await expect(
        pgslice.prep(transaction, {
          table: "posts",
          column: "created_at",
          period: "month",
          partition: true,
        }),
      ).rejects.toThrow("Table already exists: public.posts_intermediate");
    });
  });
});

describe("Pgslice.addPartitions", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15)));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe("intermediate table", () => {
    test.beforeEach(async ({ pgslice, transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "posts",
        column: "created_at",
        period: "month",
        partition: true,
      });
    });

    test("creates partitions for intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, {
        table: "posts",
        intermediate: true,
        past: 1,
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
        "posts_202512",
        "posts_202601",
        "posts_202602",
        "posts_202603",
      ]);
    });

    test("adds primary key constraint to new partitions", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, {
        table: "posts",
        intermediate: true,
        future: 1,
      });

      const constraints = await transaction.any(
        sql.type(z.object({ conname: z.string(), contype: z.string() }))`
          SELECT conname, contype FROM pg_constraint
          WHERE conrelid = 'public.posts_202601'::regclass
        `,
      );

      expect(constraints.some((c) => c.contype === "p")).toBe(true);
    });

    test("skips existing partitions (idempotent)", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.addPartitions(transaction, {
        table: "posts",
        intermediate: true,
        future: 1,
      });

      await pgslice.addPartitions(transaction, {
        table: "posts",
        intermediate: true,
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
  });

  describe("period suffixes", () => {
    test("creates day period partitions with correct suffixes", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id SERIAL PRIMARY KEY,
          occurred_at DATE NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "events",
        column: "occurred_at",
        period: "day",
        partition: true,
      });

      await pgslice.addPartitions(transaction, {
        table: "events",
        intermediate: true,
        past: 1,
        future: 1,
      });

      const partitions = await transaction.any(
        sql.type(z.object({ tablename: z.string() }))`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename LIKE 'events_2%'
          ORDER BY tablename
        `,
      );

      expect(partitions.map((p) => p.tablename)).toEqual([
        "events_20260114",
        "events_20260115",
        "events_20260116",
      ]);
    });

    test("creates year period partitions with correct suffixes", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE stats (
          id SERIAL PRIMARY KEY,
          year_date DATE NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "stats",
        column: "year_date",
        period: "year",
        partition: true,
      });

      await pgslice.addPartitions(transaction, {
        table: "stats",
        intermediate: true,
        past: 1,
        future: 1,
      });

      const partitions = await transaction.any(
        sql.type(z.object({ tablename: z.string() }))`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename LIKE 'stats_2%'
          ORDER BY tablename
        `,
      );

      expect(partitions.map((p) => p.tablename)).toEqual([
        "stats_2025",
        "stats_2026",
        "stats_2027",
      ]);
    });
  });

  describe("column types", () => {
    test("handles timestamptz columns", async ({ pgslice, transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id SERIAL PRIMARY KEY,
          occurred_at TIMESTAMPTZ NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "events",
        column: "occurred_at",
        period: "month",
        partition: true,
      });

      await pgslice.addPartitions(transaction, {
        table: "events",
        intermediate: true,
        future: 1,
      });

      const partitions = await transaction.any(
        sql.type(z.object({ tablename: z.string() }))`
          SELECT tablename FROM pg_tables
          WHERE schemaname = 'public' AND tablename LIKE 'events_2%'
        `,
      );

      expect(partitions).toEqual([
        { tablename: "events_202601" },
        { tablename: "events_202602" },
      ]);
    });
  });

  describe("error handling", () => {
    test("throws when table not found", async ({ pgslice, transaction }) => {
      await expect(
        pgslice.addPartitions(transaction, {
          table: "nonexistent",
          intermediate: true,
        }),
      ).rejects.toThrow("Table not found: public.nonexistent_intermediate");
    });

    test("throws when no settings found (suggests --intermediate)", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await expect(
        pgslice.addPartitions(transaction, {
          table: "posts",
          intermediate: false,
        }),
      ).rejects.toThrow(
        /No settings found.*Did you mean to use --intermediate/s,
      );
    });

    test("throws when no settings found (intermediate)", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      const error = await pgslice
        .addPartitions(transaction, {
          table: "posts",
          intermediate: true,
        })
        .catch((e) => e);

      expect(error.message).toBe(
        "No settings found: public.posts_intermediate",
      );
      expect(error.message).not.toContain("--intermediate");
    });
  });
});

describe("Pgslice.synchronize", () => {
  test("synchronizes data", async ({ pgslice, transaction }) => {
    // Create source table with data
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
    `);

    // Create intermediate table
    await pgslice.prep(transaction, {
      table: "posts",
      partition: false,
    });

    // Fill initial data
    for await (const _batch of pgslice.fill({ table: "posts" })) {
      // consume
    }

    // Modify source to create a difference
    await transaction.query(sql.unsafe`
      UPDATE posts SET name = 'updated_a' WHERE id = 1
    `);

    // Synchronize
    const batches = [];
    for await (const batch of pgslice.synchronize({ table: "posts" })) {
      batches.push(batch);
    }

    expect(batches).toEqual([expect.objectContaining({ rowsUpdated: 1 })]);

    // Verify the difference was fixed
    const row = await transaction.one(
      sql.type(z.object({ name: z.string() }))`
        SELECT name FROM posts_intermediate WHERE id = 1
      `,
    );
    expect(row.name).toBe("updated_a");
  });

  test("throws for missing source table", async ({ pgslice, transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_intermediate (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const error = await (async () => {
      for await (const _batch of pgslice.synchronize({ table: "posts" })) {
        // should not reach here
      }
    })().catch((e) => e);

    expect(error.message).toBe("Table not found: public.posts");
  });

  test("throws for missing target table", async ({ pgslice, transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO posts (name) VALUES ('test')
    `);

    const error = await (async () => {
      for await (const _batch of pgslice.synchronize({ table: "posts" })) {
        // should not reach here
      }
    })().catch((e) => e);

    expect(error.message).toBe("Table not found: public.posts_intermediate");
  });
});
