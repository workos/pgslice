import { test as baseTest } from "vitest";
import { Pgslice } from "../pgslice.js";
import { createPool, DatabaseTransactionConnection } from "slonik";
import { PassThrough } from "node:stream";
import { Context } from "../commands/base.js";

class TestRollbackError extends Error {
  constructor() {
    super("Intentional rollback for test isolation");
  }
}

function getTestDatabaseUrl(): string {
  const url = process.env.PGSLICE_URL;
  if (!url) {
    throw new Error("PGSLICE_URL environment variable must be set for tests");
  }

  return url;
}

export const commandTest = baseTest.extend<{
  pgslice: Pgslice;
  transaction: DatabaseTransactionConnection;
  commandContext: Context & { stdout: PassThrough };
}>({
  transaction: async ({}, use) => {
    const connection = await createPool(getTestDatabaseUrl().toString());

    try {
      await connection.transaction(async (transaction) => {
        await use(transaction);
        throw new TestRollbackError();
      });
    } catch (error) {
      if (!(error instanceof TestRollbackError)) throw error;
    }
  },

  pgslice: async ({ transaction }, use) => {
    const pgslice = new Pgslice(transaction, {});

    await use(pgslice);

    await pgslice.close();
  },

  commandContext: async ({ pgslice }, use) => {
    const stdout = new PassThrough();

    use({
      stdin: process.stdin,
      stdout,
      stderr: process.stderr,
      env: process.env,
      colorDepth: 1,
      pgslice,
    });
  },
});
