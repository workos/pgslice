import { CommonQueryMethods, DatabaseTransactionConnection } from "slonik";

import { Table } from "./table.js";
import { sql } from "./sql-utils.js";

export type MirroringTargetType = "intermediate" | "retired";

interface MirroringOptions {
  source: Table;
  targetType: MirroringTargetType;
}

/**
 * Manages database mirroring triggers between a source table and a target table.
 * These triggers ensure that INSERT, UPDATE, and DELETE operations on the source
 * table are automatically replicated to the target table.
 *
 * When the source is partitioned, Postgres clones the trigger onto every
 * partition, so changing it touches the parent and all of its partitions. Every
 * operation here locks the parent first. A statement already running finishes
 * with the locks it holds, and no new statement can take the parent and then
 * wait on a partition, which is the cycle that makes a bare `DROP TRIGGER` on a
 * busy partitioned parent deadlock against live traffic.
 */
export class Mirroring {
  readonly #source: Table;
  readonly #targetType: MirroringTargetType;

  constructor(options: MirroringOptions) {
    this.#source = options.source;
    this.#targetType = options.targetType;
  }

  /**
   * Enables mirroring by creating (or refreshing) the trigger function and
   * creating the trigger on the source table. A trigger that exists but was
   * disabled is re-enabled in place rather than dropped and recreated.
   */
  async enable(
    tx: DatabaseTransactionConnection,
    target: Table,
  ): Promise<void> {
    const columns = (await this.#source.columns(tx)).map((c) => c.name);
    const primaryKeyColumns = await this.#source.primaryKey(tx);

    const functionSql = this.#buildFunctionSql(
      columns,
      primaryKeyColumns,
      target,
    );

    await tx.query(sql.typeAlias("void")`${functionSql}`);

    if (await this.exists(tx)) {
      await this.#setEnabled(tx, true);
    } else {
      await tx.query(sql.typeAlias("void")`${this.#createTriggerSql}`);
    }
  }

  /**
   * Disables mirroring by turning the trigger off. The trigger and its function
   * stay in place, so mirroring can be re-enabled and the only lock taken is
   * SHARE ROW EXCLUSIVE on each relation, parent first: reads are never blocked
   * and writes wait only for the statement itself.
   */
  async disable(tx: DatabaseTransactionConnection): Promise<void> {
    if (!(await this.exists(tx))) {
      return;
    }

    await this.#setEnabled(tx, false);
  }

  /**
   * Removes the trigger and its function. Locks the source first: `DROP
   * TRIGGER` on a partitioned table otherwise locks the partitions before the
   * parent and deadlocks against statements entering through the parent.
   */
  async drop(tx: DatabaseTransactionConnection): Promise<void> {
    await tx.query(
      sql.typeAlias("void")`
        LOCK TABLE ${this.#source.sqlIdentifier} IN ACCESS EXCLUSIVE MODE
      `,
    );
    await tx.query(sql.typeAlias("void")`${this.#dropTriggerSql}`);
    await tx.query(sql.typeAlias("void")`${this.#dropFunctionSql}`);
  }

  /**
   * Whether the mirroring trigger exists on the source table, enabled or not.
   */
  async exists(connection: CommonQueryMethods): Promise<boolean> {
    return this.#source.triggerExists(
      connection,
      Mirroring.triggerNameFor(this.#source, this.#targetType),
    );
  }

  /**
   * Whether the mirroring trigger exists on the source table and fires.
   */
  async enabled(connection: CommonQueryMethods): Promise<boolean> {
    return this.#source.triggerEnabled(
      connection,
      Mirroring.triggerNameFor(this.#source, this.#targetType),
    );
  }

  async #setEnabled(
    tx: DatabaseTransactionConnection,
    enabled: boolean,
  ): Promise<void> {
    const action = enabled ? sql.fragment`ENABLE` : sql.fragment`DISABLE`;

    // ALTER TABLE ... ENABLE/DISABLE TRIGGER on a partitioned table recurses
    // to the cloned triggers on its partitions.
    await tx.query(
      sql.typeAlias("void")`
        ALTER TABLE ${this.#source.sqlIdentifier} ${action} TRIGGER ${this.#triggerName}
      `,
    );
  }

  get #functionName() {
    const suffix =
      this.#targetType === "intermediate"
        ? "mirror_to_intermediate"
        : "mirror_to_retired";
    return sql.identifier([`${this.#source.name}_${suffix}`]);
  }

  static triggerNameFor(table: Table, targetType: MirroringTargetType): string {
    const suffix =
      targetType === "intermediate"
        ? "mirror_trigger"
        : "retired_mirror_trigger";
    return `${table.name}_${suffix}`;
  }

  get #triggerName() {
    return sql.identifier([
      Mirroring.triggerNameFor(this.#source, this.#targetType),
    ]);
  }

  #buildWhereClause(columns: string[]) {
    return sql.join(
      columns.map(
        (col) =>
          sql.fragment`${sql.identifier([col])} = OLD.${sql.identifier([col])}`,
      ),
      sql.fragment` AND `,
    );
  }

  #buildSetClause(columns: string[]) {
    return sql.join(
      columns.map(
        (col) =>
          sql.fragment`${sql.identifier([col])} = NEW.${sql.identifier([col])}`,
      ),
      sql.fragment`, `,
    );
  }

  #buildColumnList(columns: string[]) {
    return sql.join(
      columns.map((col) => sql.identifier([col])),
      sql.fragment`, `,
    );
  }

  #buildNewTupleList(columns: string[]) {
    return sql.join(
      columns.map((col) => sql.fragment`NEW.${sql.identifier([col])}`),
      sql.fragment`, `,
    );
  }

  #buildConflictClause(columns: string[], primaryKeyColumn: string) {
    if (this.#targetType === "intermediate") {
      return sql.fragment``;
    }

    const conflictTarget = this.#buildColumnList([primaryKeyColumn]);
    const setClause = this.#buildSetClause(columns);
    return sql.fragment` ON CONFLICT (${conflictTarget}) DO UPDATE SET ${setClause}`;
  }

  #buildFunctionSql(
    columns: string[],
    primaryKeyColumn: string,
    target: Table,
  ) {
    const whereClause = this.#buildWhereClause([primaryKeyColumn]);
    const setClause = this.#buildSetClause(columns);
    const columnList = this.#buildColumnList(columns);
    const newTupleList = this.#buildNewTupleList(columns);
    const targetTable = target.sqlIdentifier;
    const conflictClause = this.#buildConflictClause(columns, primaryKeyColumn);

    return sql.fragment`
      CREATE OR REPLACE FUNCTION ${this.#functionName}()
      RETURNS TRIGGER AS $$
      BEGIN
        IF TG_OP = 'DELETE' THEN
          DELETE FROM ${targetTable} WHERE ${whereClause};
          RETURN OLD;
        ELSIF TG_OP = 'UPDATE' THEN
          UPDATE ${targetTable} SET ${setClause} WHERE ${whereClause};
          RETURN NEW;
        ELSIF TG_OP = 'INSERT' THEN
          INSERT INTO ${targetTable} (${columnList}) VALUES (${newTupleList})${conflictClause};
          RETURN NEW;
        END IF;
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql
    `;
  }

  get #dropTriggerSql() {
    return sql.fragment`
      DROP TRIGGER IF EXISTS ${this.#triggerName} ON ${this.#source.sqlIdentifier}
    `;
  }

  get #createTriggerSql() {
    return sql.fragment`
      CREATE TRIGGER ${this.#triggerName}
      AFTER INSERT OR UPDATE OR DELETE ON ${this.#source.sqlIdentifier}
      FOR EACH ROW EXECUTE FUNCTION ${this.#functionName}()
    `;
  }

  get #dropFunctionSql() {
    return sql.fragment`
      DROP FUNCTION IF EXISTS ${this.#functionName}()
    `;
  }
}
