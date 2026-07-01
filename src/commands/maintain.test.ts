import { describe, expect } from "vitest";
import { sql, type DatabaseTransactionConnection } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { MaintainCommand } from "./maintain.js";

interface LogEntry {
  jobId: string;
  msg: string;
  level: string;
  target: { db: string; host: string; schema?: string; table?: string };
  future?: { daily: number; weekly: number; monthly: number; yearly: number };
  partitions?: { new: number; total: number };
  success?: number;
  succeeded?: { count: number; tables: string[] };
  failed?: { count: number; tables: string[] };
}

/** Parse the command's stdout as JSONL (one log record per line). */
function jsonLines(output: string | undefined): LogEntry[] {
  return (output ?? "")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line): LogEntry => JSON.parse(line));
}

describe("MaintainCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(MaintainCommand) });

  async function createPosts(transaction: DatabaseTransactionConnection) {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id bigint NOT NULL,
        created_at timestamp without time zone NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    await transaction.query(sql.unsafe`
      COMMENT ON TABLE posts IS 'column:created_at,period:month,cast:date,version:3'
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_y2026m01 PARTITION OF posts
      FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
    `);
  }

  test("emits JSONL and exits 0 on a healthy fleet", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(transaction);

    const exitCode = await cli.run(["maintain"], commandContext);
    expect(exitCode).toBe(0);

    const logs = jsonLines(commandContext.stdout.read()?.toString());

    // Every record shares the shape, uses only info/error, and never leaks the
    // partitioning model.
    for (const entry of logs) {
      expect(typeof entry.msg).toBe("string");
      expect(["info", "error"]).toContain(entry.level);
      expect(entry.target.db).toBeTypeOf("string");
      expect(entry.target.host).toBe("localhost");
      expect(JSON.stringify(entry)).not.toContain("model");
    }

    // Every record from one run shares a single generated jobId (a UUID).
    const jobIds = new Set(logs.map((entry) => entry.jobId));
    expect(jobIds.size).toBe(1);
    expect([...jobIds][0]).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
    );

    const start = logs[0];
    expect(start.msg).toBe("Running pgslice maintain");
    expect(start.level).toBe("info");
    expect(start.future).toEqual({
      daily: 90,
      weekly: 26,
      monthly: 6,
      yearly: 1,
    });

    const table = logs.find((entry) => entry.target.table === "posts");
    expect(table?.msg).toBe("Extended table successfully");
    expect(table?.level).toBe("info");
    expect(table?.success).toBe(1);
    expect(table?.target.schema).toBe("public");
    expect(table?.partitions).toEqual({
      new: expect.any(Number),
      total: expect.any(Number),
    });

    const final = logs.at(-1);
    expect(final?.msg).toBe("Finished pgslice maintain successfully");
    expect(final?.level).toBe("info");
    expect(final?.succeeded).toEqual({ count: 1, tables: ["public.posts"] });
    expect(final?.failed).toEqual({ count: 0, tables: [] });
  });

  test("exits 1 and logs an error record when a leaf has no usable replica identity", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(transaction);
    await transaction.query(sql.unsafe`
      ALTER TABLE posts_y2026m01 REPLICA IDENTITY NOTHING
    `);

    const exitCode = await cli.run(["maintain"], commandContext);
    expect(exitCode).toBe(1);

    const logs = jsonLines(commandContext.stdout.read()?.toString());

    const table = logs.find((entry) => entry.target.table === "posts");
    expect(table?.level).toBe("error");
    expect(table?.success).toBe(0);
    expect(table?.msg).toContain("replica identity");
    expect(table?.target.host).toBe("localhost");

    const final = logs.at(-1);
    expect(final?.msg).toBe("Finished pgslice maintain with errors");
    expect(final?.level).toBe("error");
    expect(final?.failed).toEqual({ count: 1, tables: ["public.posts"] });
  });

  test("exits 0 with an empty summary when no managed tables exist", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE plain (id bigint NOT NULL, created_at timestamptz NOT NULL)
      PARTITION BY RANGE (created_at)
    `);

    const exitCode = await cli.run(["maintain"], commandContext);
    expect(exitCode).toBe(0);

    const logs = jsonLines(commandContext.stdout.read()?.toString());
    expect(logs[0].msg).toBe("Running pgslice maintain");
    expect(logs.some((entry) => entry.target.table !== undefined)).toBe(false);

    const final = logs.at(-1);
    expect(final?.msg).toBe("Finished pgslice maintain successfully");
    expect(final?.succeeded).toEqual({ count: 0, tables: [] });
    expect(final?.failed).toEqual({ count: 0, tables: [] });
  });

  test("logs a no-op record when a table already has runway beyond the horizon", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    // A managed parent whose only partition is far in the future: the forward
    // horizon is already covered, so maintain creates nothing.
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id bigint NOT NULL,
        created_at timestamp without time zone NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    await transaction.query(sql.unsafe`
      COMMENT ON TABLE posts IS 'column:created_at,period:month,cast:date,version:3'
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_y2099m01 PARTITION OF posts
      FOR VALUES FROM ('2099-01-01') TO ('2099-02-01')
    `);

    const exitCode = await cli.run(["maintain"], commandContext);
    expect(exitCode).toBe(0);

    const logs = jsonLines(commandContext.stdout.read()?.toString());
    const table = logs.find((entry) => entry.target.table === "posts");
    expect(table?.msg).toBe("Table already up to date; no extension needed");
    expect(table?.level).toBe("info");
    expect(table?.success).toBe(1);
    expect(table?.partitions).toEqual({ new: 0, total: 1 });
    expect(table?.target.host).toBe("localhost");
  });
});
