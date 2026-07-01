import { describe, expect } from "vitest";
import { sql, type DatabaseTransactionConnection } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { MaintainCommand } from "./maintain.js";

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

  test("exits 0 and reports a per-table summary on a healthy fleet", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(transaction);

    const exitCode = await cli.run(["maintain"], commandContext);

    expect(exitCode).toBe(0);
    expect(commandContext.stdout.read()?.toString()).toContain(
      "public.posts [native]",
    );
  });

  test("exits 1 when a leaf has no usable replica identity", async ({
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
    expect(commandContext.stderr.read()?.toString()).toContain(
      "usable replica identity",
    );
  });

  test("reports no managed tables when none carry a settings comment", async ({
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
    expect(commandContext.stdout.read()?.toString()).toContain(
      "No managed partitioned tables found.",
    );
  });
});
