import { describe, expect } from "vitest";
import { PassThrough } from "node:stream";

import { commandTest as test } from "../testing/index.js";
import { AddPartitionsCommand } from "./add-partitions.js";

describe("AddPartitionsCommand", () => {
  test("returns error when --past is negative", async ({ commandContext }) => {
    const command = new AddPartitionsCommand();
    command.context = commandContext;
    command.table = "posts";
    command.intermediate = true;
    command.past = "-1";
    command.future = "0";

    const exitCode = await command.execute();

    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("--past must be a non-negative integer");
  });

  test("returns error when --future is negative", async ({
    commandContext,
  }) => {
    const command = new AddPartitionsCommand();
    command.context = commandContext;
    command.table = "posts";
    command.intermediate = true;
    command.past = "0";
    command.future = "-1";

    const exitCode = await command.execute();

    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("--future must be a non-negative integer");
  });

  test("returns error when --past is not a number", async ({
    commandContext,
  }) => {
    const command = new AddPartitionsCommand();
    command.context = commandContext;
    command.table = "posts";
    command.intermediate = true;
    command.past = "abc";
    command.future = "0";

    const exitCode = await command.execute();

    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("--past must be a non-negative integer");
  });

  test("returns error when --future is not a number", async ({
    commandContext,
  }) => {
    const command = new AddPartitionsCommand();
    command.context = commandContext;
    command.table = "posts";
    command.intermediate = true;
    command.past = "0";
    command.future = "xyz";

    const exitCode = await command.execute();

    expect(exitCode).toBe(1);
    const output = (commandContext.stderr as PassThrough).read()?.toString();
    expect(output).toContain("--future must be a non-negative integer");
  });
});
