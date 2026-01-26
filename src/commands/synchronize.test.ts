import { describe, expect } from "vitest";
import { sql } from "slonik";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { SynchronizeCommand } from "./synchronize.js";

describe("SynchronizeCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(SynchronizeCommand) });

  describe("validation errors", () => {
    test("returns error when window size is not a number", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["synchronize", "posts", "--window-size", "abc"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'Invalid value for --window-size: expected a number (got "abc")',
      );
    });

    test("returns error when window size is zero", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["synchronize", "posts", "--window-size", "0"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        "Invalid value for --window-size: expected to be at least 1 (got 0)",
      );
    });

    test("returns error when delay is not a number", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["synchronize", "posts", "--delay", "abc"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'Invalid value for --delay: expected a number (got "abc")',
      );
    });

    test("returns error when delay-multiplier is not a number", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["synchronize", "posts", "--delay-multiplier", "abc"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'Invalid value for --delay-multiplier: expected a number (got "abc")',
      );
    });
  });

  describe("output", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          name TEXT
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (
          id INTEGER PRIMARY KEY,
          name TEXT
        )
      `);
    });

    test("outputs summary when tables match", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert same data in both tables
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b'), ('c')
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts_intermediate (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Synchronizing posts to posts_intermediate");
      expect(stderr).toContain("Mode: WRITE (executing changes)");
      expect(stderr).toContain("All 3 rows match");
      expect(stderr).toContain("Synchronization complete");
      expect(stderr).toContain("Matching rows: 3");
      expect(stderr).toContain("Rows with differences: 0");
      expect(stderr).toContain("Missing rows: 0");
      expect(stderr).toContain("Extra rows: 0");
    });

    test("outputs differences found", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Source has data, target is empty
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a'), ('b')
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Found 2 differences");
      expect(stderr).toContain("Missing rows: 2");
    });

    test("shows dry run mode", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('a')
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts", "--dry-run"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Mode: DRY RUN (logging only)");
    });

    test("outputs batch progress", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert 15 rows to have 2 batches with window size of 10
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name)
        SELECT 'item_' || i FROM generate_series(1, 15) AS i
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts", "--window-size", "10"],
        commandContext,
      );

      expect(exitCode).toBe(0);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Batch 1:");
      expect(stderr).toContain("Batch 2:");
      expect(stderr).toContain("Total batches: 2");
    });
  });

  describe("error handling", () => {
    test("returns error when source table does not exist", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Only create intermediate table
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id INTEGER PRIMARY KEY, name TEXT)
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(1);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Table not found: public.posts");
    });

    test("returns error when target table does not exist", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id SERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        INSERT INTO posts (name) VALUES ('test')
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(1);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("Table not found: public.posts_intermediate");
    });

    test("returns error when source table is empty", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (id SERIAL PRIMARY KEY, name TEXT)
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE posts_intermediate (id INTEGER PRIMARY KEY, name TEXT)
      `);

      const exitCode = await cli.run(
        ["synchronize", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(1);

      const stderr = (commandContext.stderr as PassThrough).read()?.toString();
      expect(stderr).toContain("No rows found in source table");
    });
  });
});
