import { describe, expect } from "vitest";
import { sql, type DatabaseTransactionConnection } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { DropPartitionsCommand } from "./drop-partitions.js";

describe("DropPartitionsCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(DropPartitionsCommand) });

  async function createPosts(
    transaction: DatabaseTransactionConnection,
    comment: string,
  ) {
    await transaction.query(sql.unsafe`
      CREATE TABLE posts (
        id bigint NOT NULL,
        created_at timestamp without time zone NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    await transaction.query(
      sql.unsafe`COMMENT ON TABLE posts IS ${sql.literalValue(comment)}`,
    );
    await transaction.query(sql.unsafe`
      CREATE TABLE posts_202001 PARTITION OF posts
      FOR VALUES FROM ('2020-01-01') TO ('2020-02-01')
    `);
  }

  test("--dry-run lists expired partitions without dropping them", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(
      transaction,
      "column:created_at,period:month,cast:date,version:3,retention:3",
    );

    const exitCode = await cli.run(
      ["drop_partitions", "posts", "--dry-run"],
      commandContext,
    );

    expect(exitCode).toBe(0);
    expect(commandContext.stdout.read()?.toString()).toBe(
      "posts: would drop 1 partition(s): posts_202001\n",
    );
    const exists = await transaction.oneFirst(
      sql.unsafe`SELECT to_regclass('posts_202001') IS NOT NULL`,
    );
    expect(exists).toBe(true);
  });

  test("drops expired partitions", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(
      transaction,
      "column:created_at,period:month,cast:date,version:3,retention:3",
    );

    const exitCode = await cli.run(
      ["drop_partitions", "posts"],
      commandContext,
    );

    expect(exitCode).toBe(0);
    expect(commandContext.stdout.read()?.toString()).toBe(
      "posts: dropped 1 partition(s): posts_202001\n",
    );
  });

  test("fails without a retention setting", async ({
    cli,
    commandContext,
    transaction,
  }) => {
    await createPosts(
      transaction,
      "column:created_at,period:month,cast:date,version:3",
    );

    const exitCode = await cli.run(
      ["drop_partitions", "posts"],
      commandContext,
    );

    expect(exitCode).toBe(1);
    expect(commandContext.stderr.read()?.toString()).toContain(
      "No retention configured: public.posts",
    );
  });
});
