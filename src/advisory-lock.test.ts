import { describe, expect } from "vitest";
import { createPool, sql } from "slonik";

import { pgsliceTest as test } from "./testing/index.js";
import { AdvisoryLock, AdvisoryLockError } from "./advisory-lock.js";
import { Table } from "./table.js";

describe("AdvisoryLock.withLock", () => {
  test("executes handler and returns result", async ({ transaction }) => {
    const table = Table.parse("test_table");
    const result = await AdvisoryLock.withLock(
      transaction,
      table,
      "test_op",
      async () => {
        return "success";
      },
    );

    expect(result).toBe("success");
  });

  test("propagates the handler's error and stays re-acquirable in the transaction", async ({
    transaction,
  }) => {
    const table = Table.parse("test_table");

    await expect(
      AdvisoryLock.withLock(transaction, table, "test_op", async () => {
        throw new Error("handler error");
      }),
    ).rejects.toThrow("handler error");

    // Re-acquirable within the same transaction (xact advisory locks are re-entrant)
    const result = await AdvisoryLock.withLock(
      transaction,
      table,
      "test_op",
      async () => "acquired again",
    );
    expect(result).toBe("acquired again");
  });

  test("throws AdvisoryLockError when lock is held by another session", async ({
    databaseUrl,
  }) => {
    const table = Table.parse("test_table");
    const operation = "test_op";

    // Create two separate pools - each will hold a separate session
    const pool1 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });
    const pool2 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });

    try {
      // Use a transaction in pool1 to hold the connection open while we hold the lock
      await pool1.transaction(async (tx1) => {
        // Acquire lock in the first session
        const release = await AdvisoryLock.acquire(tx1, table, operation);

        // Try to acquire the same lock in the second session
        await pool2.transaction(async (tx2) => {
          await expect(
            AdvisoryLock.acquire(tx2, table, operation),
          ).rejects.toThrow(AdvisoryLockError);
        });

        await release();
      });
    } finally {
      await pool1.end();
      await pool2.end();
    }
  });

  test("does not leak the lock when the handler aborts the transaction", async ({
    databaseUrl,
  }) => {
    const table = Table.parse("test_table");
    const operation = "test_op";

    const pool1 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });
    const pool2 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });

    try {
      // The handler aborts its transaction with a failing statement. A
      // session-scoped lock survived the rollback (leaked); a transaction-scoped
      // lock is released when the transaction rolls back.
      await expect(
        pool1.transaction((tx1) =>
          AdvisoryLock.withLock(tx1, table, operation, async () => {
            await tx1.query(sql.unsafe`SELECT 1 FROM pgslice_missing_relation`);
          }),
        ),
      ).rejects.toThrow();

      // A different session can now take the lock — proof it was released.
      await pool2.transaction(async (tx2) => {
        const result = await AdvisoryLock.withLock(
          tx2,
          table,
          operation,
          async () => "acquired after rollback",
        );
        expect(result).toBe("acquired after rollback");
      });
    } finally {
      await pool1.end();
      await pool2.end();
    }
  });
});

describe("AdvisoryLock.acquire", () => {
  test("returns a release function", async ({ transaction }) => {
    const table = Table.parse("test_table");
    const release = await AdvisoryLock.acquire(transaction, table, "test_op");

    expect(typeof release).toBe("function");
    await release();
  });

  test("same table + different operation = different locks", async ({
    databaseUrl,
  }) => {
    const table = Table.parse("test_table");

    const pool1 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });
    const pool2 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });

    try {
      // Use transactions to hold connections open
      await pool1.transaction(async (tx1) => {
        // Acquire lock for operation1
        const release1 = await AdvisoryLock.acquire(tx1, table, "operation1");

        // Should be able to acquire lock for operation2 on same table in different session
        await pool2.transaction(async (tx2) => {
          const release2 = await AdvisoryLock.acquire(tx2, table, "operation2");
          await release2();
        });

        await release1();
      });
    } finally {
      await pool1.end();
      await pool2.end();
    }
  });

  test("different table + same operation = different locks", async ({
    databaseUrl,
  }) => {
    const table1 = Table.parse("table_one");
    const table2 = Table.parse("table_two");

    const pool1 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });
    const pool2 = await createPool(databaseUrl.toString(), {
      maximumPoolSize: 1,
      queryRetryLimit: 0,
    });

    try {
      // Use transactions to hold connections open
      await pool1.transaction(async (tx1) => {
        // Acquire lock for table1
        const release1 = await AdvisoryLock.acquire(tx1, table1, "same_op");

        // Should be able to acquire lock for table2 with same operation
        await pool2.transaction(async (tx2) => {
          const release2 = await AdvisoryLock.acquire(tx2, table2, "same_op");
          await release2();
        });

        await release1();
      });
    } finally {
      await pool1.end();
      await pool2.end();
    }
  });
});

describe("AdvisoryLockError", () => {
  test("has descriptive error message", () => {
    const table = Table.parse("my_schema.my_table");
    const error = new AdvisoryLockError(table, "prep");

    expect(error.message).toContain("prep");
    expect(error.message).toContain("my_schema.my_table");
    expect(error.message).toContain("Another pgslice operation");
    expect(error.name).toBe("AdvisoryLockError");
  });
});
