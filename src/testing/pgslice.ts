import { test as baseTest } from "vitest";
import { Pgslice } from "../pgslice.js";
import { createPool, DatabaseTransactionConnection } from "slonik";

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

export const pgsliceTest = baseTest.extend<{
  pgslice: Pgslice;
  transaction: DatabaseTransactionConnection;
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
    const pgslice = new Pgslice(transaction, {
      advisoryLocks: false,
    });

    await use(pgslice);

    await pgslice.close();
  },
});
