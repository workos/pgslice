import { test as baseTest } from "vitest";
import { Pgslice } from "../pgslice.js";
import { createPool, DatabaseTransactionConnection } from "slonik";

class TestRollbackError extends Error {
  constructor() {
    super("Intentional rollback for test isolation");
  }
}

function getTestDatabaseUrl(): URL {
  const url = process.env.PGSLICE_URL;
  if (!url) {
    throw new Error("PGSLICE_URL environment variable must be set for tests");
  }

  return new URL(url);
}

export const pgsliceTest = baseTest.extend<{
  databaseUrl: URL;
  pgslice: Pgslice;
  transaction: DatabaseTransactionConnection;
}>({
  databaseUrl: getTestDatabaseUrl(),

  transaction: async ({ databaseUrl }, use) => {
    const connection = await createPool(databaseUrl.toString());

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
    const pgslice = new Pgslice(transaction, {
      // Disable advisory locks since we run tests both transcationally
      // and concurrently, which these would otherwise interfere with.
      advisoryLocks: false,
    });

    await use(pgslice);

    await pgslice.close();
  },
});
