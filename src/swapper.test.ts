import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { pgsliceTest as test } from "./testing/index.js";
import { Swapper } from "./swapper.js";
import { Table } from "./table.js";

describe("Swapper", () => {
  describe("forward direction (swap)", () => {
    describe("validation", () => {
      test("throws when original table does not exist", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table not found: public.posts",
        );
      });

      test("throws when intermediate table does not exist", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table not found: public.posts_intermediate",
        );
      });

      test("throws when retired table already exists", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table already exists: public.posts_retired",
        );
      });
    });

    describe("table renames", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("renames original table to retired", async ({ transaction }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const retired = Table.parse("posts_retired");
        expect(await retired.exists(transaction)).toBe(true);
      });

      test("renames intermediate table to original name", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const posts = Table.parse("posts");
        expect(await posts.exists(transaction)).toBe(true);
      });

      test("intermediate table no longer exists after swap", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const intermediate = Table.parse("posts_intermediate");
        expect(await intermediate.exists(transaction)).toBe(false);
      });

      test("preserves data during rename", async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Original Post')
        `);
        await transaction.query(sql.unsafe`
          INSERT INTO posts_intermediate (title) VALUES ('Intermediate Post')
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const retiredData = await transaction.one(
          sql.type(z.object({ title: z.string() }))`
            SELECT title FROM posts_retired WHERE title = 'Original Post'
          `,
        );
        expect(retiredData.title).toBe("Original Post");

        const mainData = await transaction.one(
          sql.type(z.object({ title: z.string() }))`
            SELECT title FROM posts WHERE title = 'Intermediate Post'
          `,
        );
        expect(mainData.title).toBe("Intermediate Post");
      });
    });

    describe("sequence ownership", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("transfers sequence ownership to new main table", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Pre-swap post')
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Post-swap post')
        `);

        const result = await transaction.one(
          sql.type(z.object({ count: z.coerce.number() }))`
            SELECT COUNT(*)::integer AS count FROM posts WHERE title = 'Post-swap post'
          `,
        );

        expect(result.count).toBe(1);
      });
    });

    describe("mirroring triggers", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("disables intermediate mirroring trigger if it exists", async ({
        pgslice,
        transaction,
      }) => {
        await pgslice.enableMirroring(transaction, { table: "posts" });

        const beforeResult = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_mirror_trigger'
          `,
        );
        expect(beforeResult).not.toBeNull();

        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const afterResult = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_mirror_trigger'
          `,
        );
        expect(afterResult).toBeNull();
      });

      test("creates retired mirroring trigger", async ({ transaction }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const result = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_retired_mirror_trigger'
          `,
        );
        expect(result).not.toBeNull();
      });

      test("retired trigger mirrors inserts to retired table", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Test Post')
        `);

        const result = await transaction.one(
          sql.type(z.object({ count: z.coerce.number() }))`
            SELECT COUNT(*)::integer AS count FROM posts_retired WHERE title = 'Test Post'
          `,
        );
        expect(result.count).toBe(1);
      });
    });

    describe("lock timeout", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("uses default lock timeout of 5s", async ({ transaction }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const result = await transaction.one(
          sql.type(z.object({ lock_timeout: z.string() }))`
            SHOW lock_timeout
          `,
        );
        expect(result.lock_timeout).toBe("5s");
      });

      test("uses custom lock timeout when specified", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "forward",
          lockTimeout: "10s",
        });

        await swapper.execute(transaction);

        const result = await transaction.one(
          sql.type(z.object({ lock_timeout: z.string() }))`
            SHOW lock_timeout
          `,
        );
        expect(result.lock_timeout).toBe("10s");
      });
    });

    describe("schema handling", () => {
      test.beforeEach(async ({ transaction }) => {
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
      });

      test("works with schema-qualified table names", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "myschema.posts",
          direction: "forward",
        });

        await swapper.execute(transaction);

        const posts = Table.parse("myschema.posts");
        const retired = Table.parse("myschema.posts_retired");
        const intermediate = Table.parse("myschema.posts_intermediate");

        expect(await posts.exists(transaction)).toBe(true);
        expect(await retired.exists(transaction)).toBe(true);
        expect(await intermediate.exists(transaction)).toBe(false);
      });
    });
  });

  describe("reverse direction (unswap)", () => {
    describe("validation", () => {
      test("throws when original table does not exist", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table not found: public.posts",
        );
      });

      test("throws when retired table does not exist", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table not found: public.posts_retired",
        );
      });

      test("throws when intermediate table already exists", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_intermediate (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await expect(swapper.execute(transaction)).rejects.toThrow(
          "Table already exists: public.posts_intermediate",
        );
      });
    });

    describe("table renames", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("renames original table to intermediate", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const intermediate = Table.parse("posts_intermediate");
        expect(await intermediate.exists(transaction)).toBe(true);
      });

      test("renames retired table to original name", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const posts = Table.parse("posts");
        expect(await posts.exists(transaction)).toBe(true);
      });

      test("retired table no longer exists after unswap", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const retired = Table.parse("posts_retired");
        expect(await retired.exists(transaction)).toBe(false);
      });

      test("preserves data during rename", async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Current Post')
        `);
        await transaction.query(sql.unsafe`
          INSERT INTO posts_retired (title) VALUES ('Retired Post')
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const intermediateData = await transaction.one(
          sql.type(z.object({ title: z.string() }))`
            SELECT title FROM posts_intermediate WHERE title = 'Current Post'
          `,
        );
        expect(intermediateData.title).toBe("Current Post");

        const mainData = await transaction.one(
          sql.type(z.object({ title: z.string() }))`
            SELECT title FROM posts WHERE title = 'Retired Post'
          `,
        );
        expect(mainData.title).toBe("Retired Post");
      });
    });

    describe("sequence ownership", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("transfers sequence ownership to new main table", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          INSERT INTO posts_retired (title) VALUES ('Pre-unswap post')
        `);

        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Post-unswap post')
        `);

        const result = await transaction.one(
          sql.type(z.object({ count: z.coerce.number() }))`
            SELECT COUNT(*)::integer AS count
            FROM posts
            WHERE title IN ('Pre-unswap post', 'Post-unswap post')
          `,
        );

        expect(result.count).toBe(2);
      });
    });

    describe("mirroring triggers", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("disables retired mirroring trigger if it exists", async ({
        transaction,
      }) => {
        await transaction.query(sql.unsafe`
          CREATE OR REPLACE FUNCTION posts_mirror_to_retired()
          RETURNS TRIGGER AS $$
          BEGIN
            IF TG_OP = 'INSERT' THEN
              INSERT INTO posts_retired (id, title) VALUES (NEW.id, NEW.title);
              RETURN NEW;
            END IF;
            RETURN NULL;
          END;
          $$ LANGUAGE plpgsql
        `);
        await transaction.query(sql.unsafe`
          CREATE TRIGGER posts_retired_mirror_trigger
          AFTER INSERT OR UPDATE OR DELETE ON posts
          FOR EACH ROW EXECUTE FUNCTION posts_mirror_to_retired()
        `);

        const beforeResult = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_retired_mirror_trigger'
          `,
        );
        expect(beforeResult).not.toBeNull();

        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const afterResult = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_retired_mirror_trigger'
          `,
        );
        expect(afterResult).toBeNull();
      });

      test("creates intermediate mirroring trigger", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const result = await transaction.maybeOne(
          sql.type(z.object({ tgname: z.string() }))`
            SELECT tgname FROM pg_trigger
            WHERE tgname = 'posts_mirror_trigger'
          `,
        );
        expect(result).not.toBeNull();
      });

      test("intermediate trigger mirrors inserts to intermediate table", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        await transaction.query(sql.unsafe`
          INSERT INTO posts (title) VALUES ('Test Post')
        `);

        const result = await transaction.one(
          sql.type(z.object({ count: z.coerce.number() }))`
            SELECT COUNT(*)::integer AS count FROM posts_intermediate WHERE title = 'Test Post'
          `,
        );
        expect(result.count).toBe(1);
      });
    });

    describe("lock timeout", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`
          CREATE TABLE posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("uses default lock timeout of 5s", async ({ transaction }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const result = await transaction.one(
          sql.type(z.object({ lock_timeout: z.string() }))`
            SHOW lock_timeout
          `,
        );
        expect(result.lock_timeout).toBe("5s");
      });

      test("uses custom lock timeout when specified", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "posts",
          direction: "reverse",
          lockTimeout: "15s",
        });

        await swapper.execute(transaction);

        const result = await transaction.one(
          sql.type(z.object({ lock_timeout: z.string() }))`
            SHOW lock_timeout
          `,
        );
        expect(result.lock_timeout).toBe("15s");
      });
    });

    describe("schema handling", () => {
      test.beforeEach(async ({ transaction }) => {
        await transaction.query(sql.unsafe`CREATE SCHEMA myschema`);
        await transaction.query(sql.unsafe`
          CREATE TABLE myschema.posts (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
        await transaction.query(sql.unsafe`
          CREATE TABLE myschema.posts_retired (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL
          )
        `);
      });

      test("works with schema-qualified table names", async ({
        transaction,
      }) => {
        const swapper = new Swapper({
          table: "myschema.posts",
          direction: "reverse",
        });

        await swapper.execute(transaction);

        const posts = Table.parse("myschema.posts");
        const intermediate = Table.parse("myschema.posts_intermediate");
        const retired = Table.parse("myschema.posts_retired");

        expect(await posts.exists(transaction)).toBe(true);
        expect(await intermediate.exists(transaction)).toBe(true);
        expect(await retired.exists(transaction)).toBe(false);
      });
    });
  });

  describe("roundtrip (swap then unswap)", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
    });

    test("restores original table state", async ({ transaction }) => {
      const posts = Table.parse("posts");
      const intermediate = Table.parse("posts_intermediate");
      const retired = Table.parse("posts_retired");

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(true);
      expect(await retired.exists(transaction)).toBe(false);

      const forwardSwapper = new Swapper({
        table: "posts",
        direction: "forward",
      });
      await forwardSwapper.execute(transaction);

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(false);
      expect(await retired.exists(transaction)).toBe(true);

      const reverseSwapper = new Swapper({
        table: "posts",
        direction: "reverse",
      });
      await reverseSwapper.execute(transaction);

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(true);
      expect(await retired.exists(transaction)).toBe(false);
    });

    test("preserves data through roundtrip", async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title) VALUES ('Original Post')
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (title) VALUES ('Intermediate Post')
      `);

      const forwardSwapper = new Swapper({
        table: "posts",
        direction: "forward",
      });
      await forwardSwapper.execute(transaction);

      const reverseSwapper = new Swapper({
        table: "posts",
        direction: "reverse",
      });
      await reverseSwapper.execute(transaction);

      const originalData = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::integer AS count FROM posts WHERE title = 'Original Post'
        `,
      );
      expect(originalData.count).toBe(1);

      const intermediateData = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::integer AS count FROM posts_intermediate WHERE title = 'Intermediate Post'
        `,
      );
      expect(intermediateData.count).toBe(1);
    });
  });
});
