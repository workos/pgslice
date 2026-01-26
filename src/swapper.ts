import { DatabaseTransactionConnection, sql } from "slonik";
import { z } from "zod";

import { Mirroring } from "./mirroring.js";
import { Table } from "./table.js";
import type { SwapDirection } from "./types.js";

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
    await this.#disableMirroring(tx);
    await this.#renameTables(tx);
    await this.#updateSequences(tx);
    await this.#enableMirroring(tx);
  }

  get #intermediate(): Table {
    return this.#table.intermediate();
  }

  get #retired(): Table {
    return this.#table.retired();
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
    // Original table must always exist
    if (!(await this.#table.exists(tx))) {
      throw new Error(`Table not found: ${this.#table.toString()}`);
    }

    // Source table must exist (intermediate for forward, retired for reverse)
    if (!(await this.#sourceTable.exists(tx))) {
      throw new Error(`Table not found: ${this.#sourceTable.toString()}`);
    }

    // Target table must NOT exist (retired for forward, intermediate for reverse)
    if (await this.#targetTable.exists(tx)) {
      throw new Error(`Table already exists: ${this.#targetTable.toString()}`);
    }
  }

  async #setLockTimeout(tx: DatabaseTransactionConnection): Promise<void> {
    await tx.query(
      sql.type(z.object({}))`
        SET LOCAL lock_timeout = ${sql.literalValue(this.#lockTimeout)}
      `,
    );
  }

  /**
   * Disables the appropriate mirroring trigger before renaming.
   * Forward: disables intermediate trigger
   * Reverse: disables retired trigger
   */
  async #disableMirroring(tx: DatabaseTransactionConnection): Promise<void> {
    const mode = this.#direction === "forward" ? "intermediate" : "retired";
    const target =
      this.#direction === "forward" ? this.#intermediate : this.#retired;

    await new Mirroring({
      source: this.#table,
      target,
      mode,
    }).disable(tx);
  }

  /**
   * Renames tables to perform the swap.
   * Forward: original → retired, intermediate → original
   * Reverse: original → intermediate, retired → original
   */
  async #renameTables(tx: DatabaseTransactionConnection): Promise<void> {
    // Rename original to target (retired for forward, intermediate for reverse)
    await tx.query(
      sql.type(z.object({}))`
        ALTER TABLE ${this.#table.toSqlIdentifier()} RENAME TO ${sql.identifier([this.#targetTable.name])}
      `,
    );

    // Rename source to original (intermediate for forward, retired for reverse)
    await tx.query(
      sql.type(z.object({}))`
        ALTER TABLE ${this.#sourceTable.toSqlIdentifier()} RENAME TO ${sql.identifier([this.#table.name])}
      `,
    );
  }

  /**
   * Updates sequence ownership to point to the new main table.
   * After rename, sequences are attached to the target table, so query from there.
   */
  async #updateSequences(tx: DatabaseTransactionConnection): Promise<void> {
    // After rename, the sequences are still attached to the target table
    // (retired for forward, intermediate for reverse)
    const sequences = await this.#targetTable.sequences(tx);
    for (const seq of sequences) {
      await tx.query(
        sql.type(z.object({}))`
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
   *
   * Note: The Ruby implementation has a bug where unswap doesn't re-enable
   * the intermediate trigger. This implementation fixes that bug.
   */
  async #enableMirroring(tx: DatabaseTransactionConnection): Promise<void> {
    const mode = this.#direction === "forward" ? "retired" : "intermediate";
    const target =
      this.#direction === "forward" ? this.#retired : this.#intermediate;

    // After swap, table now refers to the new main table
    await new Mirroring({
      source: this.#table,
      target,
      mode,
    }).enable(tx);
  }
}
