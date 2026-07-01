import { CommonQueryMethods } from "slonik";
import { z } from "zod";
import { Table } from "./table.js";
import { sql } from "./sql-utils.js";

export class AdvisoryLockError extends Error {
  override name = "AdvisoryLockError";

  constructor(table: Table, operation: string) {
    super(
      `Could not acquire advisory lock for "${operation}" on table "${table.toString()}". ` +
        `Another pgslice operation may be in progress.`,
    );
  }
}

export abstract class AdvisoryLock {
  /**
   * Runs a handler while holding a transaction-scoped advisory lock. The lock is
   * acquired on the given transaction and released automatically when that
   * transaction commits or rolls back — so a handler that aborts the transaction
   * cannot leak the lock, and there is no unlock query that could fail on an
   * aborted transaction and mask the handler's error.
   *
   * Must be called inside a transaction (every pgslice command wraps it in
   * `connection.transaction(...)`). For a lock that must span multiple
   * transactions — e.g. a batched generator — use {@link acquire} instead.
   */
  static async withLock<T>(
    connection: CommonQueryMethods,
    table: Table,
    operation: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const key = await this.#getKey(connection, table, operation);
    if (!(await this.#tryAcquireXact(connection, key))) {
      throw new AdvisoryLockError(table, operation);
    }
    return handler();
  }

  /**
   * Acquires an advisory lock and returns a release function.
   * Use this for generators that need to hold a lock across yields.
   */
  static async acquire(
    connection: CommonQueryMethods,
    table: Table,
    operation: string,
  ): Promise<() => Promise<void>> {
    const key = await this.#getKey(connection, table, operation);
    const acquired = await this.#tryAcquire(connection, key);

    if (!acquired) {
      throw new AdvisoryLockError(table, operation);
    }

    return async () => {
      await this.#release(connection, key);
    };
  }

  static async #getKey(
    connection: CommonQueryMethods,
    table: Table,
    operation: string,
  ): Promise<bigint> {
    const lockName = `${table.toString()}:${operation}`;
    const result = await connection.one(
      sql.type(z.object({ key: z.coerce.bigint() }))`
        SELECT hashtext(${lockName})::bigint AS key
      `,
    );
    return result.key;
  }

  static async #tryAcquire(
    connection: CommonQueryMethods,
    key: bigint,
  ): Promise<boolean> {
    const result = await connection.one(
      sql.type(z.object({ acquired: z.boolean() }))`
        SELECT pg_try_advisory_lock(${key}) AS acquired
      `,
    );
    return result.acquired;
  }

  static async #tryAcquireXact(
    connection: CommonQueryMethods,
    key: bigint,
  ): Promise<boolean> {
    const result = await connection.one(
      sql.type(z.object({ acquired: z.boolean() }))`
        SELECT pg_try_advisory_xact_lock(${key}) AS acquired
      `,
    );
    return result.acquired;
  }

  static async #release(
    connection: CommonQueryMethods,
    key: bigint,
  ): Promise<void> {
    const { acquired } = await connection.one(
      sql.type(
        z.object({ acquired: z.boolean() }),
      )`SELECT pg_advisory_unlock(${key}) AS acquired`,
    );
    if (!acquired) {
      throw new Error("Attempted to release lock that was never held.");
    }
  }
}
