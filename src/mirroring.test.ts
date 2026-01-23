import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";

describe("Pgslice.enableMirroring", () => {
  test.beforeEach(async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT,
        created_at DATE NOT NULL
      )
    `);

    await transaction.query(sql.unsafe`
      CREATE TABLE posts_intermediate (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT,
        created_at DATE NOT NULL
      )
    `);
  });

  describe("trigger creation", () => {
    test("creates trigger function with correct name", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      const count = await transaction.oneFirst(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM pg_proc
          WHERE proname = 'posts_mirror_to_intermediate'
        `,
      );
      expect(count).toBe(1);
    });

    test("creates trigger with correct name", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      const count = await transaction.oneFirst(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM pg_trigger
          WHERE tgname = 'posts_mirror_trigger'
        `,
      );
      expect(count).toBe(1);
    });

    test("is idempotent (can be called multiple times)", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });
      await pgslice.enableMirroring(transaction, { table: "posts" });

      const count = await transaction.oneFirst(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM pg_trigger
          WHERE tgname = 'posts_mirror_trigger'
        `,
      );
      expect(count).toBe(1);
    });
  });

  describe("trigger behavior", () => {
    test("trigger fires on INSERT - row appears in intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, body, created_at) VALUES ('Hello', 'World', '2024-01-15')
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string(), body: z.string() }))`
          SELECT title, body FROM posts_intermediate WHERE title = 'Hello'
        `,
      );
      expect(result).toEqual({ title: "Hello", body: "World" });
    });

    test("trigger fires on UPDATE - row updated in intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, body, created_at) VALUES ('Hello', 'World', '2024-01-15')
      `);

      await transaction.query(sql.unsafe`
        UPDATE posts SET title = 'Updated' WHERE title = 'Hello'
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string(), body: z.string() }))`
          SELECT title, body FROM posts_intermediate WHERE body = 'World'
        `,
      );
      expect(result).toEqual({ title: "Updated", body: "World" });
    });

    test("trigger fires on DELETE - row deleted from intermediate table", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, body, created_at) VALUES ('Hello', 'World', '2024-01-15')
      `);

      await transaction.query(sql.unsafe`
        DELETE FROM posts WHERE title = 'Hello'
      `);

      const count = await transaction.oneFirst(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::int FROM posts_intermediate
        `,
      );
      expect(count).toBe(0);
    });
  });

  describe("primary key handling", () => {
    test("uses primary key for WHERE clause when available", async ({
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, body, created_at) VALUES ('Hello', 'World', '2024-01-15')
      `);

      const postId = await transaction.oneFirst(
        sql.type(z.object({ id: z.coerce.number() }))`
          SELECT id FROM posts WHERE title = 'Hello'
        `,
      );

      await transaction.query(sql.unsafe`
        UPDATE posts SET title = 'Changed', body = 'Changed' WHERE id = ${postId}
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string(), body: z.string() }))`
          SELECT title, body FROM posts_intermediate WHERE id = ${postId}
        `,
      );
      expect(result).toEqual({ title: "Changed", body: "Changed" });
    });

    test("falls back to all columns when no primary key", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`DROP TABLE posts_intermediate`);
      await transaction.query(sql.unsafe`DROP TABLE posts`);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          title TEXT NOT NULL,
          body TEXT,
          created_at DATE NOT NULL
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          title TEXT NOT NULL,
          body TEXT,
          created_at DATE NOT NULL
        )
      `);

      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, body, created_at) VALUES ('Hello', 'World', '2024-01-15')
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string(), body: z.string() }))`
          SELECT title, body FROM posts_intermediate
        `,
      );
      expect(result).toEqual({ title: "Hello", body: "World" });
    });

    test("handles composite primary keys", async ({ pgslice, transaction }) => {
      await transaction.query(sql.unsafe`DROP TABLE posts_intermediate`);
      await transaction.query(sql.unsafe`DROP TABLE posts`);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          tenant_id INTEGER NOT NULL,
          id INTEGER NOT NULL,
          title TEXT NOT NULL,
          PRIMARY KEY (tenant_id, id)
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          tenant_id INTEGER NOT NULL,
          id INTEGER NOT NULL,
          title TEXT NOT NULL,
          PRIMARY KEY (tenant_id, id)
        )
      `);

      await pgslice.enableMirroring(transaction, { table: "posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO posts (tenant_id, id, title) VALUES (1, 100, 'Hello')
      `);

      await transaction.query(sql.unsafe`
        UPDATE posts SET title = 'Updated' WHERE tenant_id = 1 AND id = 100
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string() }))`
          SELECT title FROM posts_intermediate WHERE tenant_id = 1 AND id = 100
        `,
      );
      expect(result).toEqual({ title: "Updated" });
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
          title TEXT NOT NULL
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE myschema.posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);

      await pgslice.enableMirroring(transaction, { table: "myschema.posts" });

      await transaction.query(sql.unsafe`
        INSERT INTO myschema.posts (title) VALUES ('Hello')
      `);

      const result = await transaction.one(
        sql.type(z.object({ title: z.string() }))`
          SELECT title FROM myschema.posts_intermediate
        `,
      );
      expect(result).toEqual({ title: "Hello" });
    });
  });

  describe("error handling", () => {
    test("throws when source table not found", async ({
      pgslice,
      transaction,
    }) => {
      await expect(
        pgslice.enableMirroring(transaction, { table: "nonexistent" }),
      ).rejects.toThrow("Table not found: public.nonexistent");
    });

    test("throws when intermediate table not found", async ({
      pgslice,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`DROP TABLE posts_intermediate`);

      await expect(
        pgslice.enableMirroring(transaction, { table: "posts" }),
      ).rejects.toThrow("Table not found: public.posts_intermediate");
    });
  });
});
