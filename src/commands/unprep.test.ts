import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { UnprepCommand } from "./unprep.js";
import { Table } from "../table.js";

describe("UnprepCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(UnprepCommand) });

  describe("errors", () => {
    test("returns error when intermediate table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(["unprep", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts_intermediate");
    });

    test("returns error when intermediate table not found with schema", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`CREATE SCHEMA myschema`);

      const exitCode = await cli.run(
        ["unprep", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: myschema.posts_intermediate");
    });
  });

  describe("success", () => {
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

    test("outputs success message", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(["unprep", "posts"], commandContext);

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Dropped intermediate table for posts");
    });

    test("drops intermediate table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unprep", "posts"], commandContext);

      const intermediate = Table.parse("posts_intermediate");
      expect(await intermediate.exists(transaction)).toBe(false);
    });

    test("preserves original table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await cli.run(["unprep", "posts"], commandContext);

      const posts = Table.parse("posts");
      expect(await posts.exists(transaction)).toBe(true);
    });
  });

  describe("cascade", () => {
    test("drops partitions attached to intermediate table", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id INT NOT NULL,
          created_at DATE NOT NULL
        ) PARTITION BY RANGE (created_at)
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_20240101 PARTITION OF posts_intermediate
        FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_20240201 PARTITION OF posts_intermediate
        FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')
      `);

      const exitCode = await cli.run(["unprep", "posts"], commandContext);
      expect(exitCode).toBe(0);

      const intermediate = Table.parse("posts_intermediate");
      const partition1 = Table.parse("posts_20240101");
      const partition2 = Table.parse("posts_20240201");

      expect(await intermediate.exists(transaction)).toBe(false);
      expect(await partition1.exists(transaction)).toBe(false);
      expect(await partition2.exists(transaction)).toBe(false);
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
        ["unprep", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const posts = Table.parse("myschema.posts");
      const intermediate = Table.parse("myschema.posts_intermediate");

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(false);
    });
  });

  describe("roundtrip", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);
    });

    test("prep then unprep restores original state", async ({
      pgslice,
      transaction,
    }) => {
      const posts = Table.parse("posts");
      const intermediate = Table.parse("posts_intermediate");

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(false);

      await pgslice.prep(transaction, {
        table: "posts",
        partition: true,
        column: "created_at",
        period: "month",
      });

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(true);

      await pgslice.unprep(transaction, { table: "posts" });

      expect(await posts.exists(transaction)).toBe(true);
      expect(await intermediate.exists(transaction)).toBe(false);
    });
  });
});
