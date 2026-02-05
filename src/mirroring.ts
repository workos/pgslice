import { DatabaseTransactionConnection } from "slonik";

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
 */
export class Mirroring {
  readonly #source: Table;
  readonly #targetType: MirroringTargetType;

  constructor(options: MirroringOptions) {
    this.#source = options.source;
    this.#targetType = options.targetType;
  }

  /**
   * Enables mirroring by creating a trigger function and trigger on the source table.
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
    await tx.query(sql.typeAlias("void")`${this.#dropTriggerSql}`);
    await tx.query(sql.typeAlias("void")`${this.#createTriggerSql}`);
  }

  /**
   * Disables mirroring by dropping the trigger and function.
   */
  async disable(tx: DatabaseTransactionConnection): Promise<void> {
    await tx.query(sql.typeAlias("void")`${this.#dropTriggerSql}`);
    await tx.query(sql.typeAlias("void")`${this.#dropFunctionSql}`);
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
