import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { AddPartitionsCommand } from "./add-partitions.js";

describe("AddPartitionsCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(AddPartitionsCommand) });

  test("reports the partitions it created", async ({
    cli,
    commandContext,
    transaction,
  }) => {
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

    const exitCode = await cli.run(
      ["add_partitions", "posts", "--future=1"],
      commandContext,
    );

    expect(exitCode).toBe(0);
    expect(commandContext.stdout.read()?.toString()).toContain("posts: +");
  });

  test("returns error when --past is negative", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["add_partitions", "posts", "--intermediate", "--past=-1", "--future=0"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stdout.read()?.toString();
    expect(output).toContain(
      "Invalid value for --past: expected to be at least 0 (got -1)",
    );
  });

  test("returns error when --future is negative", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["add_partitions", "posts", "--intermediate", "--past=0", "--future=-1"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stdout.read()?.toString();
    expect(output).toContain(
      "Invalid value for --future: expected to be at least 0 (got -1)",
    );
  });

  test("returns error when --past is not a number", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["add_partitions", "posts", "--intermediate", "--past=abc", "--future=0"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stdout.read()?.toString();
    expect(output).toContain(
      'Invalid value for --past: expected a number (got "abc")',
    );
  });

  test("returns error when --future is not a number", async ({
    cli,
    commandContext,
  }) => {
    const exitCode = await cli.run(
      ["add_partitions", "posts", "--intermediate", "--past=0", "--future=xyz"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    const output = commandContext.stdout.read()?.toString();
    expect(output).toContain(
      'Invalid value for --future: expected a number (got "xyz")',
    );
  });
});
