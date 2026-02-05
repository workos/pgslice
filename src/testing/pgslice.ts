import { test as baseTest, vi } from "vitest";
import { Pgslice } from "../pgslice.js";
import {
  createPool,
  DatabasePool,
  DatabaseTransactionConnection,
} from "slonik";

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
  pool: DatabasePool;
  transaction: DatabaseTransactionConnection;
}>({
  databaseUrl: getTestDatabaseUrl(),

  pool: async ({ databaseUrl }, use) => {
    const pool = await createPool(databaseUrl.toString());
    try {
      await use(pool);
    } finally {
      await pool.end();
    }
  },

  transaction: async ({ pool }, use) => {
    try {
      await pool.transaction(async (transaction) => {
        await use(transaction);
        throw new TestRollbackError();
      });
    } catch (error) {
      if (!(error instanceof TestRollbackError)) throw error;
    }
  },

  pgslice: async ({ pool, transaction }, use) => {
    const transactionalizedPool = {
      ...transaction,
      configuration: pool.configuration,
      connect: vi.fn().mockImplementation((handler) => handler(transaction)),
      end: vi.fn().mockResolvedValue(undefined),
      state: vi.fn().mockReturnValue(pool.state),

      // A bunch of event emitter stuff that we don't use but having this
      // makes the compiler helper.
      addListener: vi.fn().mockReturnThis(),
      emit: vi.fn().mockReturnValue(false),
      eventNames: vi.fn().mockReturnValue([]),
      getMaxListeners: vi.fn().mockReturnValue(0),
      listenerCount: vi.fn().mockReturnValue(0),
      listeners: vi.fn().mockReturnThis(),
      off: vi.fn().mockReturnThis(),
      on: vi.fn().mockReturnThis(),
      once: vi.fn().mockReturnThis(),
      prependListener: vi.fn().mockReturnThis(),
      prependOnceListener: vi.fn().mockReturnThis(),
      rawListeners: vi.fn().mockReturnThis(),
      removeAllListeners: vi.fn().mockReturnThis(),
      removeListener: vi.fn().mockReturnThis(),
      setMaxListeners: vi.fn().mockReturnThis(),
    } satisfies DatabasePool;

    const pgslice = new Pgslice(transactionalizedPool, {
      // Disable advisory locks since we run tests both transactionally
      // and concurrently, which these would otherwise interfere with.
      advisoryLocks: false,
    });

    await use(pgslice);

    await pgslice.close();
  },
});
