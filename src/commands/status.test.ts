import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { StatusCommand } from "./status.js";

describe("StatusCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(StatusCommand) });

  test.beforeEach(async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        created_at DATE NOT NULL
      )
    `);
  });

  test("outputs status as JSON", async ({ cli, commandContext }) => {
    const exitCode = await cli.run(
      ["status", "posts", "--json"],
      commandContext,
    );

    expect(exitCode).toBe(0);
    const status = JSON.parse(commandContext.stdout.read()?.toString() ?? "");
    expect(status).toEqual({
      intermediateExists: false,
      partitionCount: 0,
      mirrorTriggerExists: false,
      retiredMirrorTriggerExists: false,
      originalIsPartitioned: false,
    });
  });

  test("outputs human-readable status", async ({ cli, commandContext }) => {
    const exitCode = await cli.run(["status", "posts"], commandContext);

    expect(exitCode).toBe(0);
    const output = commandContext.stdout.read()?.toString();
    expect(output).toContain("Table: posts");
    expect(output).toContain("Intermediate exists:");
    expect(output).toContain("Original is partitioned:");
  });
});
