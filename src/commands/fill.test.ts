import { describe, expect } from "vitest";
import { sql } from "slonik";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { FillCommand } from "./fill.js";

describe("FillCommand", () => {
  describe("validation errors", () => {
    test("returns error when batch size is not a number", async ({
      commandContext,
    }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "abc";

      const exitCode = await command.execute();

      expect(exitCode).toBe(1);
      const output = (commandContext.stderr as PassThrough).read()?.toString();
      expect(output).toContain("Invalid batch size");
    });

    test("returns error when batch size is zero", async ({ commandContext }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "0";

      const exitCode = await command.execute();

      expect(exitCode).toBe(1);
      const output = (commandContext.stderr as PassThrough).read()?.toString();
      expect(output).toContain("Invalid batch size");
    });

    test("returns error when batch size is negative", async ({
      commandContext,
    }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "-1";

      const exitCode = await command.execute();

      expect(exitCode).toBe(1);
      const output = (commandContext.stderr as PassThrough).read()?.toString();
      expect(output).toContain("Invalid batch size");
    });

    test("returns error when sleep value is not a number", async ({
      commandContext,
    }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "10000";
      command.sleep = "abc";

      const exitCode = await command.execute();

      expect(exitCode).toBe(1);
      const output = (commandContext.stderr as PassThrough).read()?.toString();
      expect(output).toContain("Invalid sleep value");
    });

    test("returns error when sleep value is negative", async ({
      commandContext,
    }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "10000";
      command.sleep = "-1";

      const exitCode = await command.execute();

      expect(exitCode).toBe(1);
      const output = (commandContext.stderr as PassThrough).read()?.toString();
      expect(output).toContain("Invalid sleep value");
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

    test("outputs batch progress", async ({ commandContext, transaction }) => {
      // Insert 15 rows to have 2 batches with batch size of 10
      await transaction.query(sql.unsafe`
        INSERT INTO posts (created_at)
        SELECT '2025-01-15'::date
        FROM generate_series(1, 15)
      `);

      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "10";
      command.swapped = false;
      command.sourceTable = undefined;
      command.destTable = undefined;
      command.start = undefined;
      command.sleep = undefined;

      await command.execute();

      const output = (commandContext.stdout as PassThrough).read()?.toString();
      expect(output).toBeDefined();
      expect(output).toContain("/* 1 of 2 */");
      expect(output).toContain("/* 2 of 2 */");
    });

    test("outputs nothing to fill when source is empty", async ({
      commandContext,
    }) => {
      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "10000";
      command.swapped = false;
      command.sourceTable = undefined;
      command.destTable = undefined;
      command.start = undefined;
      command.sleep = undefined;

      await command.execute();

      const output = (commandContext.stdout as PassThrough).read()?.toString();
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

      const command = new FillCommand();
      command.context = commandContext;
      command.table = "events";
      command.batchSize = "2";
      command.swapped = false;
      command.sourceTable = undefined;
      command.destTable = undefined;
      command.start = undefined;
      command.sleep = undefined;

      await command.execute();

      const output = (commandContext.stdout as PassThrough).read()?.toString();
      expect(output).toBeDefined();
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
      commandContext,
      transaction,
    }) => {
      // Insert 5 rows to have 2 batches with batch size of 3
      await transaction.query(sql.unsafe`
        INSERT INTO posts (created_at)
        SELECT '2025-01-15'::date
        FROM generate_series(1, 5)
      `);

      const command = new FillCommand();
      command.context = commandContext;
      command.table = "posts";
      command.batchSize = "3";
      command.swapped = false;
      command.sourceTable = undefined;
      command.destTable = undefined;
      command.start = undefined;
      command.sleep = "0.001"; // 1ms sleep - small enough to not slow tests

      await command.execute();

      const output = (commandContext.stdout as PassThrough).read()?.toString();
      expect(output).toBeDefined();
      expect(output).toContain("/* 1 of 2 */");
      expect(output).toContain("/* 2 of 2 */");
    });
  });
});
