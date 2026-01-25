import { describe, expect } from "vitest";
import { sql } from "slonik";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { FillCommand } from "./fill.js";

describe("FillCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(FillCommand) });

  describe("validation errors", () => {
    test("returns error when batch size is not a number", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["fill", "posts", "--batch-size", "abc"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'Invalid value for --batch-size: expected a number (got "abc")',
      );
    });

    test("returns error when batch size is zero", async ({
      commandContext,
      cli,
    }) => {
      const exitCode = await cli.run(
        ["fill", "posts", "--batch-size", "0"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        "Invalid value for --batch-size: expected to be at least 1 (got 0)",
      );
    });

    test("returns error when sleep value is not a number", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["fill", "posts", "--batch-size", "10000", "--sleep", "abc"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        'Invalid value for --sleep: expected a number (got "abc")',
      );
    });

    test("returns error when sleep value is negative", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["fill", "posts", "--batch-size", "10000", "--sleep=-1"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = (commandContext.stdout as PassThrough).read()?.toString();
      expect(output).toContain(
        "Invalid value for --sleep: expected to be positive (got -1)",
      );
    });
  });

  describe("output", () => {
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

      await transaction.query(sql.unsafe`
        COMMENT ON TABLE posts_intermediate IS 'column:created_at,period:month,cast:date,version:3'
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_202501
        PARTITION OF posts_intermediate
        FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')
      `);

      await transaction.query(sql.unsafe`
        ALTER TABLE posts_202501 ADD PRIMARY KEY (id)
      `);
    });

    test("outputs batch progress", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert 15 rows to have 2 batches with batch size of 10
      await transaction.query(sql.unsafe`
        INSERT INTO posts (created_at)
        SELECT '2025-01-15'::date
        FROM generate_series(1, 15)
      `);

      const exitCode = await cli.run(
        ["fill", "posts", "--batch-size", "10"],
        commandContext,
      );

      expect(expect(exitCode).toBe(0));
      const output = commandContext.stdout.read()?.toString();
      expect(output).toBeDefined();
      expect(output).toContain("/* batch 1 */");
      expect(output).toContain("/* batch 2 */");
    });

    test("outputs nothing to fill when source is empty", async ({
      commandContext,
      cli,
    }) => {
      const exitCode = await cli.run(["fill", "posts"], commandContext);

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toBeDefined();
      expect(output).toContain("/* nothing to fill */");
    });
  });

  describe("ULID batch output", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE events (
          id TEXT PRIMARY KEY,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE events_intermediate (
          id TEXT NOT NULL,
          created_at DATE NOT NULL
        ) PARTITION BY RANGE (created_at)
      `);

      await transaction.query(sql.unsafe`
        COMMENT ON TABLE events_intermediate IS 'column:created_at,period:month,cast:date,version:3'
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE events_202501
        PARTITION OF events_intermediate
        FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')
      `);

      await transaction.query(sql.unsafe`
        ALTER TABLE events_202501 ADD PRIMARY KEY (id)
      `);
    });

    test("outputs batch N without total for ULID-based tables", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert ULID-style IDs
      await transaction.query(sql.unsafe`
        INSERT INTO events (id, created_at) VALUES
        ('01HQ1234567890ABCDEFGHJ001', '2025-01-15'),
        ('01HQ1234567890ABCDEFGHJ002', '2025-01-15'),
        ('01HQ1234567890ABCDEFGHJ003', '2025-01-15')
      `);

      const exitCode = await cli.run(
        ["fill", "events", "--batch-size", "2"],
        commandContext,
      );

      expect(expect(exitCode).toBe(0));
      const output = commandContext.stdout.read()?.toString();
      // ULID batches show "batch N" without total
      expect(output).toContain("/* batch 1 */");
      expect(output).toContain("/* batch 2 */");
    });
  });

  describe("sleep behavior", () => {
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

      await transaction.query(sql.unsafe`
        COMMENT ON TABLE posts_intermediate IS 'column:created_at,period:month,cast:date,version:3'
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_202501
        PARTITION OF posts_intermediate
        FOR VALUES FROM ('2025-01-01') TO ('2025-02-01')
      `);

      await transaction.query(sql.unsafe`
        ALTER TABLE posts_202501 ADD PRIMARY KEY (id)
      `);
    });

    test("completes with sleep option set", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      // Insert 5 rows to have 2 batches with batch size of 3
      await transaction.query(sql.unsafe`
        INSERT INTO posts (created_at)
        SELECT '2025-01-15'::date
        FROM generate_series(1, 5)
      `);

      await cli.run(
        ["fill", "posts", "--batch-size", "3", "--sleep", "0.001"],
        commandContext,
      );

      const output = (commandContext.stdout as PassThrough).read()?.toString();
      expect(output).toBeDefined();
      expect(output).toContain("/* batch 1 */");
      expect(output).toContain("/* batch 2 */");
    });
  });
});
