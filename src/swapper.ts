import { DatabaseTransactionConnection } from "slonik";

import { Mirroring } from "./mirroring.js";
import { Table } from "./table.js";
import type { SwapDirection } from "./types.js";
import { sql } from "./sql-utils.js";

interface SwapperOptions {
  table: string;
  direction: SwapDirection;
  lockTimeout?: string;
}

/**
 * Handles table swapping operations for partitioning workflows.
 *
 * The Swapper manages both "forward" swaps (swap command) and "reverse" swaps
 * (unswap command), abstracting the symmetric operations:
 *
 * Forward (swap):
 * - Validates: original + intermediate exist, retired does NOT
 * - Disables intermediate mirroring trigger
 * - Renames: original → retired, intermediate → original
 * - Updates sequence ownership from retired
 * - Enables retired mirroring trigger
 *
 * Reverse (unswap):
 * - Validates: original + retired exist, intermediate does NOT
 * - Disables retired mirroring trigger
 * - Renames: original → intermediate, retired → original
 * - Updates sequence ownership from intermediate
 * - Enables intermediate mirroring trigger
 */
export class Swapper {
  readonly #table: Table;
  readonly #direction: SwapDirection;
  readonly #lockTimeout: string;

  constructor(options: SwapperOptions) {
    this.#table = Table.parse(options.table);
    this.#direction = options.direction;
    this.#lockTimeout = options.lockTimeout ?? "5s";
  }

  /**
   * Executes the swap operation within the provided transaction.
   */
  async execute(tx: DatabaseTransactionConnection): Promise<void> {
    await this.#validateTables(tx);
    await this.#setLockTimeout(tx);
    await this.#disableOldMirroring(tx);
    await this.#renameTables(tx);
    await this.#updateSequences(tx);
    await this.#enableNewMirroring(tx);
  }

  get #intermediate(): Table {
    return this.#table.intermediate;
  }

  get #retired(): Table {
    return this.#table.retired;
  }

  /**
   * The table that will become the target of rename operations.
   * Forward: intermediate → original
   * Reverse: retired → original
   */
  get #sourceTable(): Table {
    return this.#direction === "forward" ? this.#intermediate : this.#retired;
  }

  /**
   * The table that the original will be renamed to.
   * Forward: original → retired
   * Reverse: original → intermediate
   */
  get #targetTable(): Table {
    return this.#direction === "forward" ? this.#retired : this.#intermediate;
  }

  /**
   * Validates that the required tables exist and conflicting table doesn't.
   */
  async #validateTables(tx: DatabaseTransactionConnection): Promise<void> {
    if (!(await this.#table.exists(tx))) {
      throw new Error(`Table not found: ${this.#table.toString()}`);
    }

    if (!(await this.#sourceTable.exists(tx))) {
      throw new Error(`Table not found: ${this.#sourceTable.toString()}`);
    }

    if (await this.#targetTable.exists(tx)) {
      throw new Error(`Table already exists: ${this.#targetTable.toString()}`);
    }
  }

  async #setLockTimeout(tx: DatabaseTransactionConnection): Promise<void> {
    await tx.query(
      sql.typeAlias("void")`
        SET LOCAL lock_timeout = ${sql.literalValue(this.#lockTimeout)}
      `,
    );
  }

  /**
   * Disables the appropriate mirroring trigger before renaming.
   * Forward: disables intermediate trigger
   * Reverse: disables retired trigger
   */
  async #disableOldMirroring(tx: DatabaseTransactionConnection): Promise<void> {
    const targetType =
      this.#direction === "forward" ? "intermediate" : "retired";

    await new Mirroring({ source: this.#table, targetType }).disable(tx);
  }

  /**
   * Renames tables to perform the swap.
   * Forward: original → retired, intermediate → original
   * Reverse: original → intermediate, retired → original
   */
  async #renameTables(tx: DatabaseTransactionConnection): Promise<void> {
    await tx.query(
      sql.typeAlias("void")`
        ALTER TABLE ${this.#table.sqlIdentifier} RENAME TO ${sql.identifier([this.#targetTable.name])}
      `,
    );

    await tx.query(
      sql.typeAlias("void")`
        ALTER TABLE ${this.#sourceTable.sqlIdentifier} RENAME TO ${sql.identifier([this.#table.name])}
      `,
    );
  }

  /**
   * Updates sequence ownership to point to the new main table.
   * After rename, sequences are attached to the target table, so query from there.
   */
  async #updateSequences(tx: DatabaseTransactionConnection): Promise<void> {
    for (const seq of await this.#targetTable.sequences(tx)) {
      await tx.query(
        sql.typeAlias("void")`
          ALTER SEQUENCE ${sql.identifier([seq.sequenceSchema, seq.sequenceName])}
          OWNED BY ${sql.identifier([this.#table.schema, this.#table.name, seq.relatedColumn])}
        `,
      );
    }
  }

  /**
   * Enables the appropriate mirroring trigger after renaming.
   * Forward: enables retired trigger (mirrors from new main table to retired)
   * Reverse: enables intermediate trigger (mirrors from new main table to intermediate)
   */
  async #enableNewMirroring(tx: DatabaseTransactionConnection): Promise<void> {
    const targetType =
      this.#direction === "forward" ? "retired" : "intermediate";

    await new Mirroring({ source: this.#table, targetType }).enable(
      tx,
      this.#targetTable,
    );
  }
}
