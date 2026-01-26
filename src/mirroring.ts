import { DatabaseTransactionConnection, sql } from "slonik";
import { z } from "zod";

import { Table } from "./table.js";

interface MirroringOptions {
  source: Table;
  target: Table;
}

/**
 * Manages database mirroring triggers between a source table and a target table.
 * These triggers ensure that INSERT, UPDATE, and DELETE operations on the source
 * table are automatically replicated to the target table.
 */
export class Mirroring {
  readonly #source: Table;
  readonly #target: Table;

  constructor(options: MirroringOptions) {
    this.#source = options.source;
    this.#target = options.target;
  }

  /**
   * Enables mirroring by creating a trigger function and trigger on the source table.
   */
  async enable(tx: DatabaseTransactionConnection): Promise<void> {
    const columns = (await this.#source.columns(tx)).map((c) => c.name);
    const primaryKeyColumns = await this.#source.primaryKey(tx);

    const functionSql = this.#buildFunctionSql(columns, primaryKeyColumns);
    const dropTriggerSql = this.#buildDropTriggerSql();
    const createTriggerSql = this.#buildCreateTriggerSql();

    await tx.query(sql.type(z.object({}))`${functionSql}`);
    await tx.query(sql.type(z.object({}))`${dropTriggerSql}`);
    await tx.query(sql.type(z.object({}))`${createTriggerSql}`);
  }

  /**
   * Disables mirroring by dropping the trigger and function.
   */
  async disable(tx: DatabaseTransactionConnection): Promise<void> {
    const dropTriggerSql = this.#buildDropTriggerSql();
    const dropFunctionSql = this.#buildDropFunctionSql();

    await tx.query(sql.type(z.object({}))`${dropTriggerSql}`);
    await tx.query(sql.type(z.object({}))`${dropFunctionSql}`);
  }

  get #functionName() {
    return sql.identifier([`${this.#source.name}_mirror_to_intermediate`]);
  }

  get #triggerName() {
    return sql.identifier([`${this.#source.name}_mirror_trigger`]);
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

  #buildFunctionSql(columns: string[], primaryKeyColumns: string[]) {
    const whereColumns =
      primaryKeyColumns.length > 0 ? primaryKeyColumns : columns;
    const whereClause = this.#buildWhereClause(whereColumns);
    const setClause = this.#buildSetClause(columns);
    const columnList = this.#buildColumnList(columns);
    const newTupleList = this.#buildNewTupleList(columns);
    const targetTable = this.#target.toSqlIdentifier();

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
          INSERT INTO ${targetTable} (${columnList}) VALUES (${newTupleList});
          RETURN NEW;
        END IF;
        RETURN NULL;
      END;
      $$ LANGUAGE plpgsql
    `;
  }

  #buildDropTriggerSql() {
    return sql.fragment`
      DROP TRIGGER IF EXISTS ${this.#triggerName} ON ${this.#source.toSqlIdentifier()}
    `;
  }

  #buildCreateTriggerSql() {
    return sql.fragment`
      CREATE TRIGGER ${this.#triggerName}
      AFTER INSERT OR UPDATE OR DELETE ON ${this.#source.toSqlIdentifier()}
      FOR EACH ROW EXECUTE FUNCTION ${this.#functionName}()
    `;
  }

  #buildDropFunctionSql() {
    return sql.fragment`
      DROP FUNCTION IF EXISTS ${this.#functionName}()
    `;
  }
}
