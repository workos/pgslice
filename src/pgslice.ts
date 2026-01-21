import {
  CommonQueryMethods,
  createPool,
  DatabaseTransactionConnection,
  sql,
  type DatabasePool,
} from "slonik";
import { z } from "zod";

import type { Period, PrepOptions } from "./types.js";
import { SQL_FORMAT } from "./types.js";
import { Table, getServerVersionNum } from "./table.js";

interface PgsliceOptions {
  dryRun?: boolean;
}

export class Pgslice {
  #dryRun: boolean;

  constructor(
    connection: DatabasePool | CommonQueryMethods,
    options: PgsliceOptions,
  ) {
    this.#dryRun = options.dryRun ?? false;
    this.#connection = connection;
  }

  static async connect(
    databaseUrl: URL,
    options: PgsliceOptions = {},
  ): Promise<Pgslice> {
    const connection = await createPool(databaseUrl.toString());
    const instance = new Pgslice(connection, options);
    return instance;
  }

  #connection: DatabasePool | CommonQueryMethods | null = null;

  async start<T>(
    handler: (transaction: DatabaseTransactionConnection) => Promise<T>,
  ): Promise<T> {
    if (!this.#connection) {
      throw new Error("Not connected to the database");
    }

    if (this.#dryRun) {
      throw new Error("Dry run not yet supported.");
    }

    return this.#connection.transaction(handler);
  }

  async close(): Promise<void> {
    if (this.#connection) {
      if ("end" in this.#connection) {
        await this.#connection.end();
      }
      this.#connection = null;
    }
  }

  /**
   * Creates an intermediate table for partitioning.
   *
   * This is the first step in the pgslice workflow. The intermediate table
   * can be created as a partitioned table (the default) or as a regular table
   * with `partition: false`.
   */
  async prep(
    tx: DatabaseTransactionConnection,
    options: PrepOptions,
  ): Promise<void> {
    const { table: tableName, column, period, partition = true } = options;

    const table = Table.parse(tableName);
    const intermediate = table.intermediate();

    // Validation
    if (!partition) {
      if (column || period) {
        throw new Error(
          'Usage: "pgslice prep TABLE --no-partition" (column and period not allowed)',
        );
      }
    }

    if (!(await table.exists(tx))) {
      throw new Error(`Table not found: ${table.toString()}`);
    }

    if (await intermediate.exists(tx)) {
      throw new Error(`Table already exists: ${intermediate.toString()}`);
    }

    if (partition) {
      if (!column || !period) {
        throw new Error('Usage: "pgslice prep TABLE COLUMN PERIOD"');
      }

      const columns = await table.columns(tx);
      if (!columns.includes(column)) {
        throw new Error(`Column not found: ${column}`);
      }

      if (!isValidPeriod(period)) {
        throw new Error(`Invalid period: ${period}`);
      }
    }

    // Build and execute queries
    if (partition) {
      await this.#createPartitionedIntermediateTable(
        tx,
        table,
        intermediate,
        column as string,
        period as Period,
      );
    } else {
      await this.#createUnpartitionedIntermediateTable(tx, table, intermediate);
    }
  }

  async #createPartitionedIntermediateTable(
    tx: DatabaseTransactionConnection,
    table: Table,
    intermediate: Table,
    column: string,
    period: Period,
  ): Promise<void> {
    const serverVersionNum = await getServerVersionNum(tx);

    // Create partitioned table using the appropriate INCLUDING clauses
    const intermediateIdent = intermediate.toSqlIdentifier();
    const tableIdent = table.toSqlIdentifier();
    const columnIdent = sql.identifier([column]);

    // For Postgres 14+, include COMPRESSION
    if (serverVersionNum >= 140000) {
      await tx.query(
        sql.type(z.object({}))`
          CREATE TABLE ${intermediateIdent} (LIKE ${tableIdent} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS INCLUDING STATISTICS INCLUDING GENERATED INCLUDING COMPRESSION) PARTITION BY RANGE (${columnIdent})
        `,
      );
    } else {
      await tx.query(
        sql.type(z.object({}))`
          CREATE TABLE ${intermediateIdent} (LIKE ${tableIdent} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS INCLUDING STATISTICS INCLUDING GENERATED) PARTITION BY RANGE (${columnIdent})
        `,
      );
    }

    // Copy indexes
    const indexDefs = await table.indexDefs(tx);
    for (const indexDef of indexDefs) {
      // Transform the index definition to point to the intermediate table
      const transformedIndexDef = indexDef
        .replace(
          / ON \S+ USING /,
          ` ON ${intermediate.toQuotedString()} USING `,
        )
        .replace(/ INDEX .+ ON /, " INDEX ON ");
      await tx.query(rawSql(transformedIndexDef));
    }

    // Copy foreign keys
    const foreignKeys = await table.foreignKeys(tx);
    for (const fkDef of foreignKeys) {
      const fkSql = `ALTER TABLE ${intermediate.toQuotedString()} ADD ${fkDef}`;
      await tx.query(rawSql(fkSql));
    }

    // Add metadata comment
    const cast = await table.columnCast(tx, column);
    const comment = `column:${column},period:${period},cast:${cast},version:3`;
    await tx.query(
      sql.type(z.object({}))`
        COMMENT ON TABLE ${intermediateIdent} IS ${sql.literalValue(comment)}
      `,
    );
  }

  async #createUnpartitionedIntermediateTable(
    tx: DatabaseTransactionConnection,
    table: Table,
    intermediate: Table,
  ): Promise<void> {
    const intermediateIdent = intermediate.toSqlIdentifier();
    const tableIdent = table.toSqlIdentifier();

    // Create table with all properties
    await tx.query(
      sql.type(z.object({}))`
        CREATE TABLE ${intermediateIdent} (LIKE ${tableIdent} INCLUDING ALL)
      `,
    );

    // Copy foreign keys (not included with LIKE ... INCLUDING ALL)
    const foreignKeys = await table.foreignKeys(tx);
    for (const fkDef of foreignKeys) {
      const fkSql = `ALTER TABLE ${intermediate.toQuotedString()} ADD ${fkDef}`;
      await tx.query(rawSql(fkSql));
    }
  }
}

function isValidPeriod(period: string): period is Period {
  return period in SQL_FORMAT;
}

function rawSql(query: string) {
  const raw = Object.freeze([query]);
  const strings = Object.assign([query], { raw });
  return sql.unsafe(strings);
}
