import { describe, expect } from "vitest";
import { sql } from "slonik";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { PrepCommand } from "./prep.js";

describe("PrepCommand", () => {
  test("returns error when --no-partition used with column/period", async ({
    transaction,
    commandContext,
  }) => {
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

  test("returns error when partition is true but column is missing", async ({
    transaction,
    commandContext,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);

    const command = new PrepCommand();
    command.context = commandContext;
    command.table = "posts";
    command.column = undefined;
    command.period = "month";
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE COLUMN PERIOD"');
  });

  test("returns error when partition is true but period is missing", async ({
    transaction,
    commandContext,
  }) => {
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
    command.period = undefined;
    command.partition = true;

    const exitCode = await command.execute();
    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain('Usage: "pgslice prep TABLE COLUMN PERIOD"');
  });
});
