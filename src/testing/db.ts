import { test as baseTest } from "vitest";
import { Pgslice } from "../pgslice.js";
import type { DatabaseTransactionConnection } from "slonik";

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

export const test = baseTest.extend<{
  pgslice: Pgslice;
  connection: DatabaseTransactionConnection;
}>({
  pgslice: async ({}, use) => {
    const pgslice = await Pgslice.connect(new URL(getTestDatabaseUrl()), {
      dryRun: false,
    });

    await use(pgslice);

    await pgslice.close();
  },

  connection: async ({ pgslice }, use) => {
    try {
      await pgslice.connection.transaction(async (txConnection) => {
        await use(txConnection);
        throw new TestRollbackError();
      });
    } catch (error) {
      if (!(error instanceof TestRollbackError)) throw error;
    }
  },
});
