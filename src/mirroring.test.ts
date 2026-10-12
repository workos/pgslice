import { describe, expect } from "vitest";
import { DatabaseTransactionConnection, sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { Mirroring } from "./mirroring.js";
import { Table } from "./table.js";

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

describe("Pgslice.disableMirroring", () => {
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

  test("drops trigger function", async ({ pgslice, transaction }) => {
    await pgslice.enableMirroring(transaction, { table: "posts" });
    await pgslice.disableMirroring(transaction, { table: "posts" });

    const count = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_proc
        WHERE proname = 'posts_mirror_to_intermediate'
      `,
    );
    expect(count).toBe(0);
  });

  test("drops trigger", async ({ pgslice, transaction }) => {
    await pgslice.enableMirroring(transaction, { table: "posts" });
    await pgslice.disableMirroring(transaction, { table: "posts" });

    const count = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_trigger
        WHERE tgname = 'posts_mirror_trigger'
      `,
    );
    expect(count).toBe(0);
  });

  test("is idempotent (can be called when no trigger exists)", async ({
    pgslice,
    transaction,
  }) => {
    await pgslice.disableMirroring(transaction, { table: "posts" });

    const count = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_trigger
        WHERE tgname = 'posts_mirror_trigger'
      `,
    );
    expect(count).toBe(0);
  });

  test("works after enabling (enable then disable removes mirroring)", async ({
    pgslice,
    transaction,
  }) => {
    await pgslice.enableMirroring(transaction, { table: "posts" });

    await transaction.query(sql.unsafe`
      INSERT INTO posts (title, body, created_at) VALUES ('Before disable', 'Body', '2024-01-15')
    `);

    const beforeCount = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(beforeCount).toBe(1);

    await pgslice.disableMirroring(transaction, { table: "posts" });

    await transaction.query(sql.unsafe`
      INSERT INTO posts (title, body, created_at) VALUES ('After disable', 'Body', '2024-01-16')
    `);

    const afterCount = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM posts_intermediate
      `,
    );
    expect(afterCount).toBe(1);
  });

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
    await pgslice.disableMirroring(transaction, { table: "myschema.posts" });

    const count = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_proc
        WHERE proname = 'posts_mirror_to_intermediate'
      `,
    );
    expect(count).toBe(0);
  });

  describe("error handling", () => {
    test("throws when source table not found", async ({
      pgslice,
      transaction,
    }) => {
      await expect(
        pgslice.disableMirroring(transaction, { table: "nonexistent" }),
      ).rejects.toThrow("Table not found: public.nonexistent");
    });
  });
});

describe("Pgslice.disableMirroring (partitioned table)", () => {
  const triggerCount = async (
    transaction: DatabaseTransactionConnection,
    name: string,
  ) =>
    transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_trigger WHERE tgname = ${name}
      `,
    );

  test.beforeEach(async ({ pgslice, transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL,
        title TEXT NOT NULL,
        created_at DATE NOT NULL
      ) PARTITION BY RANGE (created_at)
    `);
    await transaction.query(sql.unsafe`
      CREATE UNIQUE INDEX posts_replica_identity_uidx ON posts (id, created_at)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_202401 PARTITION OF posts
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_202402 PARTITION OF posts
        FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
    `);
    await transaction.query(sql.unsafe`
      ALTER TABLE posts_202401 ADD PRIMARY KEY (id)
    `);
    await transaction.query(sql.unsafe`
      ALTER TABLE posts_202402 ADD PRIMARY KEY (id)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_retired (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        created_at DATE NOT NULL
      )
    `);
    await pgslice.enableMirroring(transaction, {
      table: "posts",
      targetType: "retired",
    });
  });

  test("clones the trigger onto every partition", async ({ transaction }) => {
    expect(
      await triggerCount(transaction, "posts_retired_mirror_trigger"),
    ).toBe(3);
  });

  test("locks the parent before it drops anything", async ({ transaction }) => {
    const issued: string[] = [];
    const recording: DatabaseTransactionConnection = {
      ...transaction,
      async query(query) {
        issued.push(query.sql.replace(/\s+/g, " ").trim());
        return transaction.query(query);
      },
    };

    await new Mirroring({
      source: Table.parse("posts"),
      targetType: "retired",
    }).disable(recording);

    expect(issued[0]).toMatch(
      /^LOCK TABLE "public"\."posts" IN ACCESS EXCLUSIVE MODE$/,
    );
    expect(issued[1]).toMatch(/^DROP TRIGGER IF EXISTS/);
    expect(issued[2]).toMatch(/^DROP FUNCTION IF EXISTS/);

    const held = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_locks
        WHERE relation = 'posts'::regclass
          AND pid = pg_backend_pid()
          AND mode = 'AccessExclusiveLock'
          AND granted
      `,
    );
    expect(held).toBe(1);
  });

  test("removes the trigger from the parent and every partition", async ({
    pgslice,
    transaction,
  }) => {
    await pgslice.disableMirroring(transaction, {
      table: "posts",
      targetType: "retired",
    });

    expect(
      await triggerCount(transaction, "posts_retired_mirror_trigger"),
    ).toBe(0);
    const functions = await transaction.oneFirst(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*)::int FROM pg_proc WHERE proname = 'posts_mirror_to_retired'
      `,
    );
    expect(functions).toBe(0);
  });

  test("bounds the lock wait with the requested lock_timeout", async ({
    pgslice,
    transaction,
  }) => {
    await pgslice.disableMirroring(transaction, {
      table: "posts",
      targetType: "retired",
      lockTimeout: "7s",
    });

    const lockTimeout = await transaction.oneFirst(
      sql.type(z.object({ lock_timeout: z.string() }))`SHOW lock_timeout`,
    );
    expect(lockTimeout).toBe("7s");
  });
});
