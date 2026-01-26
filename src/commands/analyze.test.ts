import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { AnalyzeCommand } from "./analyze.js";

describe("AnalyzeCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(AnalyzeCommand) });

  describe("table not found", () => {
    test("returns error when intermediate table does not exist", async ({
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

      const exitCode = await cli.run(["analyze", "posts"], commandContext);

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts_intermediate");
    });

    test("returns error when main table does not exist with --swapped", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["analyze", "posts", "--swapped"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts");
    });
  });

  describe("analyzes intermediate table by default", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id INTEGER NOT NULL,
          created_at DATE NOT NULL
        ) PARTITION BY RANGE (created_at)
      `);
    });

    test("outputs analyze statement for intermediate table", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(["analyze", "posts"], commandContext);

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'ANALYZE VERBOSE "public"."posts_intermediate";',
      );
    });
  });

  describe("analyzes main table with --swapped", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);
    });

    test("outputs analyze statement for main table", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["analyze", "posts", "--swapped"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain('ANALYZE VERBOSE "public"."posts";');
    });
  });

  describe("schema-qualified table names", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`CREATE SCHEMA custom_schema`);

      await transaction.query(sql.unsafe`
        CREATE TABLE custom_schema.posts (
          id SERIAL PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE custom_schema.posts_intermediate (
          id INTEGER NOT NULL,
          created_at DATE NOT NULL
        ) PARTITION BY RANGE (created_at)
      `);
    });

    test("analyzes intermediate table in custom schema", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["analyze", "custom_schema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'ANALYZE VERBOSE "custom_schema"."posts_intermediate";',
      );
    });

    test("analyzes main table in custom schema with --swapped", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["analyze", "custom_schema.posts", "--swapped"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain('ANALYZE VERBOSE "custom_schema"."posts";');
    });
  });
});
