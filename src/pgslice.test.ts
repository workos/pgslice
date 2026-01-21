import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";

describe("Pgslice.prep", () => {
  describe("partitioned tables", () => {
    test("creates a partitioned intermediate table", async ({
      pgslice,
      transaction,
    }) => {
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
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
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
      expect(result.length).toBeGreaterThan(0);
    });

    test("copies multiple indexes to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          author TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_title ON posts (title)
      `);
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_author ON posts (author)
      `);
      await transaction.query(sql.unsafe`
        CREATE INDEX idx_posts_status ON posts (status)
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
      const indexNames = result.map((r) => r.indexname);
      expect(indexNames.some((name) => name.includes("title"))).toBe(true);
      expect(indexNames.some((name) => name.includes("author"))).toBe(true);
      expect(indexNames.some((name) => name.includes("status"))).toBe(true);
    });

    test("copies foreign keys to intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE authors (id SERIAL PRIMARY KEY)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          author_id INTEGER REFERENCES authors(id),
          created_at DATE NOT NULL
        )
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
      expect(result.length).toBe(1);
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
        CREATE TABLE tags (id SERIAL PRIMARY KEY)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          author_id INTEGER REFERENCES authors(id),
          category_id INTEGER REFERENCES categories(id),
          tag_id INTEGER REFERENCES tags(id),
          created_at DATE NOT NULL
        )
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
      expect(result.length).toBe(3);
    });

    test("stores correct metadata in table comment", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

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
      await transaction.query(sql.unsafe`
        CREATE TABLE users (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL
        )
      `);

      await pgslice.prep(transaction, {
        table: "users",
        partition: false,
      });

      const result = await transaction.one(
        sql.type(z.object({ relkind: z.string() }))`
          SELECT relkind FROM pg_class
          WHERE relname = 'users_intermediate'
        `,
      );
      expect(result.relkind).toBe("r");
    });
  });

  describe("column type detection", () => {
    test("detects date column cast", async ({ pgslice, transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

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
    test("stores correct metadata with day period", async ({
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
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id SERIAL PRIMARY KEY,
          occurred_at DATE NOT NULL
        )
      `);

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
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);

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
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

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
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);
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
