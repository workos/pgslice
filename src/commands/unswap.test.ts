import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { commandTest as test } from "../testing/index.js";
import { UnswapCommand } from "./unswap.js";
import { Table } from "../table.js";

describe("UnswapCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(UnswapCommand) });

  describe("errors", () => {
    test("returns error when original table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(["unswap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts");
    });

    test("returns error when retired table not found", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);

      const exitCode = await cli.run(["unswap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts_retired");
    });

    test("returns error when intermediate table already exists", async ({
      cli,
      commandContext,
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

      const exitCode = await cli.run(["unswap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain(
        "Table already exists: public.posts_intermediate",
      );
    });
  });

  describe("success", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_retired (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
    });

    test("outputs success message", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(["unswap", "posts"], commandContext);

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Unswapped posts with retired table");
    });

    test("renames original table to intermediate", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unswap", "posts"], commandContext);

      const intermediate = Table.parse("posts_intermediate");
      expect(await intermediate.exists(transaction)).toBe(true);
    });

    test("renames retired table to original name", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unswap", "posts"], commandContext);

      const posts = Table.parse("posts");
      expect(await posts.exists(transaction)).toBe(true);
    });

    test("retired table no longer exists after unswap", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unswap", "posts"], commandContext);

      const retired = Table.parse("posts_retired");
      expect(await retired.exists(transaction)).toBe(false);
    });

    test("transfers sequence ownership to new main table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert a row into the retired table before unswap to establish the sequence value
      // After unswap, this table becomes the main table
      await transaction.query(sql.unsafe`
        INSERT INTO posts_retired (title, created_at) VALUES ('Pre-unswap post', '2024-01-01')
      `);

      await cli.run(["unswap", "posts"], commandContext);

      // Verify the sequence works on the new main table by inserting a new row
      // This implicitly tests that sequence ownership was transferred
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, created_at) VALUES ('Post-unswap post', '2024-01-02')
      `);

      // Check that both rows exist in the new main table
      const result = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::integer AS count FROM posts WHERE title IN ('Pre-unswap post', 'Post-unswap post')
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
      cli,
      commandContext,
      transaction,
    }) => {
      // Create a retired mirroring trigger manually (simulating a previous swap)
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

      // Verify trigger exists
      const beforeResult = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_retired_mirror_trigger'
        `,
      );
      expect(beforeResult).not.toBeNull();

      await cli.run(["unswap", "posts"], commandContext);

      // Verify retired trigger is removed
      const afterResult = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_retired_mirror_trigger'
        `,
      );
      expect(afterResult).toBeNull();
    });

    test("creates intermediate mirroring trigger", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unswap", "posts"], commandContext);

      // Verify intermediate mirroring trigger exists on the new main table
      const result = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_mirror_trigger'
        `,
      );
      expect(result).not.toBeNull();
    });

    test("intermediate mirroring trigger mirrors inserts to intermediate table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unswap", "posts"], commandContext);

      // Insert into the new main table (formerly retired)
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title) VALUES ('Test Post')
      `);

      // Verify the insert was mirrored to the intermediate table
      const result = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::integer AS count FROM posts_intermediate WHERE title = 'Test Post'
        `,
      );
      expect(result.count).toBe(1);
    });
  });

  describe("schema-qualified tables", () => {
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
      cli,
      commandContext,
      transaction,
    }) => {
      const exitCode = await cli.run(
        ["unswap", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const posts = Table.parse("myschema.posts");
      const intermediate = Table.parse("myschema.posts_intermediate");
      const retired = Table.parse("myschema.posts_retired");

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(true);
      expect(await retired.exists(transaction)).toBe(false);
    });
  });

  describe("options", () => {
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

    test("accepts custom lock timeout", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(
        ["unswap", "posts", "--lock-timeout", "10s"],
        commandContext,
      );

      expect(exitCode).toBe(0);
    });
  });

  describe("roundtrip", () => {
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

    test("swap then unswap restores original state", async ({
      pgslice,
      transaction,
    }) => {
      // Check initial state
      const initialPosts = Table.parse("posts");
      const initialIntermediate = Table.parse("posts_intermediate");
      const initialRetired = Table.parse("posts_retired");

      expect(await initialPosts.exists(transaction)).toBe(true);
      expect(await initialIntermediate.exists(transaction)).toBe(true);
      expect(await initialRetired.exists(transaction)).toBe(false);

      // Perform swap
      await pgslice.swap(transaction, { table: "posts" });

      // After swap: posts exists (was intermediate), intermediate gone, retired exists
      expect(await initialPosts.exists(transaction)).toBe(true);
      expect(await initialIntermediate.exists(transaction)).toBe(false);
      expect(await initialRetired.exists(transaction)).toBe(true);

      // Perform unswap
      await pgslice.unswap(transaction, { table: "posts" });

      // After unswap: should be back to original state
      expect(await initialPosts.exists(transaction)).toBe(true);
      expect(await initialIntermediate.exists(transaction)).toBe(true);
      expect(await initialRetired.exists(transaction)).toBe(false);
    });

    test("roundtrip preserves data in both tables", async ({
      pgslice,
      transaction,
    }) => {
      // Insert data into original table
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title) VALUES ('Original Post')
      `);

      // Insert data into intermediate table
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (title) VALUES ('Intermediate Post')
      `);

      // Perform swap
      await pgslice.swap(transaction, { table: "posts" });

      // Perform unswap
      await pgslice.unswap(transaction, { table: "posts" });

      // Verify data is still present in both tables
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
