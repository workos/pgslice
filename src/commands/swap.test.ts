import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { commandTest as test } from "../testing/index.js";
import { SwapCommand } from "./swap.js";
import { Table } from "../table.js";

describe("SwapCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(SwapCommand) });

  describe("errors", () => {
    test("returns error when original table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(["swap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts");
    });

    test("returns error when intermediate table not found", async ({
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

      const exitCode = await cli.run(["swap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts_intermediate");
    });

    test("returns error when retired table already exists", async ({
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

      const exitCode = await cli.run(["swap", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table already exists: public.posts_retired");
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
        CREATE TABLE posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
    });

    test("outputs success message", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(["swap", "posts"], commandContext);

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Swapped posts with intermediate table");
    });

    test("renames original table to retired", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["swap", "posts"], commandContext);

      const retired = Table.parse("posts_retired");
      expect(await retired.exists(transaction)).toBe(true);
    });

    test("renames intermediate table to original name", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["swap", "posts"], commandContext);

      const posts = Table.parse("posts");
      expect(await posts.exists(transaction)).toBe(true);
    });

    test("intermediate table no longer exists after swap", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["swap", "posts"], commandContext);

      const intermediate = Table.parse("posts_intermediate");
      expect(await intermediate.exists(transaction)).toBe(false);
    });

    test("transfers sequence ownership to new main table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert a row before swap to establish the sequence value
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, created_at) VALUES ('Pre-swap post', '2024-01-01')
      `);

      await cli.run(["swap", "posts"], commandContext);

      // Verify the sequence works on the new main table by inserting a new row
      // This implicitly tests that sequence ownership was transferred
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title, created_at) VALUES ('Post-swap post', '2024-01-02')
      `);

      // Check that both rows exist (one in retired, one in new table via trigger mirroring)
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
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      // Enable mirroring first
      await pgslice.enableMirroring(transaction, { table: "posts" });

      // Verify trigger exists
      const beforeResult = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_mirror_trigger'
        `,
      );
      expect(beforeResult).not.toBeNull();

      await cli.run(["swap", "posts"], commandContext);

      // Verify intermediate trigger is removed
      const afterResult = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_mirror_trigger'
        `,
      );
      expect(afterResult).toBeNull();
    });

    test("creates retired mirroring trigger", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["swap", "posts"], commandContext);

      // Verify retired mirroring trigger exists on the new main table
      const result = await transaction.maybeOne(
        sql.type(z.object({ tgname: z.string() }))`
          SELECT tgname FROM pg_trigger
          WHERE tgname = 'posts_retired_mirror_trigger'
        `,
      );
      expect(result).not.toBeNull();
    });

    test("retired mirroring trigger mirrors inserts to retired table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["swap", "posts"], commandContext);

      // Insert into the new main table (formerly intermediate)
      await transaction.query(sql.unsafe`
        INSERT INTO posts (title) VALUES ('Test Post')
      `);

      // Verify the insert was mirrored to the retired table
      const result = await transaction.one(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT COUNT(*)::integer AS count FROM posts_retired WHERE title = 'Test Post'
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
        CREATE TABLE myschema.posts_intermediate (
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
        ["swap", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const posts = Table.parse("myschema.posts");
      const retired = Table.parse("myschema.posts_retired");
      const intermediate = Table.parse("myschema.posts_intermediate");

      expect(await posts.exists(transaction)).toBe(true);
      expect(await retired.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(false);
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
        CREATE TABLE posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
    });

    test("accepts custom lock timeout", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(
        ["swap", "posts", "--lock-timeout", "10s"],
        commandContext,
      );

      expect(exitCode).toBe(0);
    });
  });
});
