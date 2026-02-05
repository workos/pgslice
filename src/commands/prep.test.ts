import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { PrepCommand } from "./prep.js";

describe("PrepCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(PrepCommand) });

  test.beforeEach(async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);
  });

  test("returns error when --no-partition used with column/period", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["prep", "posts", "created_at", "month", "--no-partition"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stderr.read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE --no-partition"');
  });

  test("returns error when partition is true but column is missing", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(["prep", "posts"], commandContext);

    expect(exitCode).toBe(1);
    const output = commandContext.stderr.read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE COLUMN PERIOD"');
  });

  test("returns error when partition is true but period is missing", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["prep", "posts", "created_at"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stderr.read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE COLUMN PERIOD"');
  });
});
