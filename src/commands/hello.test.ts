import { PassThrough } from "node:stream";
import { expect } from "vitest";
import { sql } from "slonik";
import { test } from "../testing/index.js";
import { HelloCommand } from "./hello.js";

test("creates a table", async ({ connection }) => {
  await connection.query(sql.unsafe`CREATE TABLE test_table (id INT)`);

  const result = await connection.one(
    sql.unsafe`SELECT COUNT(*)::int as count FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'test_table'`,
  );
  expect(result.count).toBe(1);
});

test("table does not persist (rollback worked)", async ({ connection }) => {
  const result = await connection.maybeOne(
    sql.unsafe`SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'test_table'`,
  );
  expect(result).toBeNull();
});

test("HelloCommand.perform() runs database statements", async ({ pgslice }) => {
  const stdout = new PassThrough();
  const command = new HelloCommand();
  command.context = {
    stdin: process.stdin,
    stdout,
    stderr: process.stderr,
    env: process.env,
    colorDepth: 1,
    pgslice,
  };

  await command.perform();

  const output = stdout.read()?.toString();
  expect(output).toContain("Hello from pgslice!");
});
