import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { PrepCommand } from "./prep.js";

describe("PrepCommand", () => {
  test("creates a partitioned intermediate table", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    await command.execute();

    // Verify intermediate table exists and is partitioned
    const result = await transaction.one(
      sql.type(z.object({ relkind: z.string() }))`
        SELECT relkind FROM pg_class
        WHERE relname = 'posts_intermediate'
      `,
    );
    expect(result.relkind).toBe("p"); // 'p' = partitioned table
  });

  test("creates a non-partitioned intermediate table with --no-partition", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "users";
    command.column = undefined;
    command.period = undefined;
    command.partition = false;

    await command.execute();

    // Verify intermediate table exists and is a regular table
    const result = await transaction.one(
      sql.type(z.object({ relkind: z.string() }))`
        SELECT relkind FROM pg_class
        WHERE relname = 'users_intermediate'
      `,
    );
    expect(result.relkind).toBe("r"); // 'r' = regular table
  });

  test("copies indexes to intermediate table", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table with an index
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        created_at DATE NOT NULL
      )
    `);
    await transaction.query(sql.unsafe`
      CREATE INDEX idx_posts_title ON posts (title)
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    await command.execute();

    // Verify index was copied (will be named differently on the intermediate table)
    const result = await transaction.any(
      sql.type(z.object({ indexname: z.string() }))`
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'posts_intermediate' AND indexname LIKE '%title%'
      `,
    );
    expect(result.length).toBeGreaterThan(0);
  });

  test("copies foreign keys to intermediate table", async ({
    transaction,
    commandContext,
  }) => {
    // Create referenced table and source table with foreign key
    await transaction.query(sql.unsafe`
      CREATE TABLE authors (id SERIAL PRIMARY KEY)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        author_id INTEGER REFERENCES authors(id),
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    await command.execute();

    // Verify foreign key was copied
    const result = await transaction.any(
      sql.type(z.object({ conname: z.string() }))`
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'public.posts_intermediate'::regclass AND contype = 'f'
      `,
    );
    expect(result.length).toBe(1);
  });

  test("stores correct metadata in table comment", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    await command.execute();

    // Verify comment
    const result = await transaction.one(
      sql.type(z.object({ comment: z.string().nullable() }))`
        SELECT obj_description('public.posts_intermediate'::regclass) AS comment
      `,
    );
    expect(result.comment).toBe(
      "column:created_at,period:month,cast:date,version:3",
    );
  });

  test("handles schema-qualified table names", async ({
    transaction,
    commandContext,
  }) => {
    // Create schema and table
    await transaction.query(sql.unsafe`CREATE SCHEMA myschema`);
    await transaction.query(sql.unsafe`
      CREATE TABLE myschema.posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "myschema.posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    await command.execute();

    // Verify intermediate table exists in the correct schema
    const result = await transaction.maybeOne(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*) FROM pg_tables
        WHERE schemaname = 'myschema' AND tablename = 'posts_intermediate'
      `,
    );
    expect(Number(result?.count)).toBe(1);
  });

  test("returns error when table not found", async ({ commandContext }) => {
    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "nonexistent";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("Table not found: public.nonexistent");
  });

  test("returns error when column not found", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "nonexistent_column";
    command.period = "month";
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("Column not found: nonexistent_column");
  });

  test("returns error for invalid period", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "invalid";
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("Invalid period: invalid");
  });

  test("returns error when intermediate table already exists", async ({
    transaction,
    commandContext,
  }) => {
    // Create source and intermediate tables
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_intermediate (id SERIAL PRIMARY KEY)
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("Table already exists: public.posts_intermediate");
  });

  test("returns error when --no-partition used with column/period", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = "created_at";
    command.period = "month";
    command.partition = false;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE --no-partition"');
  });

  test("detects timestamptz column cast correctly", async ({
    transaction,
    commandContext,
  }) => {
    // Create source table with timestamptz column
    await transaction.query(sql.unsafe`
      CREATE TABLE events (
        id SERIAL PRIMARY KEY,
        occurred_at TIMESTAMPTZ NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "events";
    command.column = "occurred_at";
    command.period = "day";
    command.partition = true;

    await command.execute();

    // Verify comment contains timestamptz cast
    const result = await transaction.one(
      sql.type(z.object({ comment: z.string().nullable() }))`
        SELECT obj_description('public.events_intermediate'::regclass) AS comment
      `,
    );
    expect(result.comment).toBe(
      "column:occurred_at,period:day,cast:timestamptz,version:3",
    );
  });
});
