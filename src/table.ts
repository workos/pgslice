import { DatabaseTransactionConnection, sql, IdentifierSqlToken } from "slonik";
import { z } from "zod";
import type { Cast } from "./types.js";

const DEFAULT_SCHEMA = "public";

/**
 * Gets the server version number.
 */
export async function getServerVersionNum(
  tx: DatabaseTransactionConnection,
): Promise<number> {
  const result = await tx.one(
    sql.type(z.object({ server_version_num: z.coerce.number() }))`
      SELECT current_setting('server_version_num')::integer AS server_version_num
    `,
  );
  return result.server_version_num;
}

/**
 * Represents a database table with schema and name.
 */
export class Table {
  readonly schema: string;
  readonly name: string;

  constructor(schema: string, name: string) {
    this.schema = schema;
    this.name = name;
  }

  /**
   * Parses a table name string into a Table instance.
   * If no schema is provided, defaults to "public".
   */
  static parse(name: string, defaultSchema: string = DEFAULT_SCHEMA): Table {
    if (name.includes(".")) {
      const [schema, tableName] = name.split(".", 2);
      return new Table(schema, tableName);
    }
    return new Table(defaultSchema, name);
  }

  /**
   * Creates the intermediate table derived from this table.
   */
  intermediate(): Table {
    return new Table(this.schema, `${this.name}_intermediate`);
  }

  /**
   * Creates the retired table derived from this table.
   */
  retired(): Table {
    return new Table(this.schema, `${this.name}_retired`);
  }

  /**
   * Creates a SQL identifier token for this table.
   */
  toSqlIdentifier(): IdentifierSqlToken {
    return sql.identifier([this.schema, this.name]);
  }

  /**
   * Returns the quoted string representation of this table for use in SQL.
   */
  toLiteralValue() {
    return sql.literalValue(this.toString());
  }

  /**
   * Returns the string representation of this table (schema.name).
   */
  toString(): string {
    return `${this.schema}.${this.name}`;
  }

  /**
   * Checks if this table exists in the database.
   */
  async exists(tx: DatabaseTransactionConnection): Promise<boolean> {
    const result = await tx.one(sql.type(
      z.object({ count: z.coerce.number() }),
    )`
      SELECT COUNT(*) FROM pg_catalog.pg_tables
      WHERE schemaname = ${this.schema} AND tablename = ${this.name}
    `);
    return result.count > 0;
  }

  /**
   * Gets the list of column names for this table (excluding generated columns).
   */
  async columns(tx: DatabaseTransactionConnection): Promise<string[]> {
    const result = await tx.any(
      sql.type(z.object({ column_name: z.string() }))`
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = ${this.schema}
          AND table_name = ${this.name}
          AND is_generated = 'NEVER'
      `,
    );
    return result.map((row) => row.column_name);
  }

  /**
   * Gets the cast type for a column (date or timestamptz).
   */
  async columnCast(
    tx: DatabaseTransactionConnection,
    column: string,
  ): Promise<Cast> {
    const result = await tx.maybeOne(
      sql.type(z.object({ data_type: z.string() }))`
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = ${this.schema}
          AND table_name = ${this.name}
          AND column_name = ${column}
      `,
    );
    if (!result) {
      throw new Error(`Column not found: ${column}`);
    }
    return result.data_type === "timestamp with time zone"
      ? "timestamptz"
      : "date";
  }

  /**
   * Gets index definitions for this table (excluding primary key).
   */
  async indexDefs(tx: DatabaseTransactionConnection): Promise<string[]> {
    const result = await tx.any(
      sql.type(z.object({ pg_get_indexdef: z.string() }))`
        SELECT pg_get_indexdef(indexrelid) FROM pg_index
        WHERE indrelid = ${this.toLiteralValue()}::regclass AND indisprimary = 'f'
      `,
    );
    return result.map((row) => row.pg_get_indexdef);
  }

  /**
   * Gets foreign key constraint definitions for this table.
   */
  async foreignKeys(tx: DatabaseTransactionConnection): Promise<string[]> {
    const result = await tx.any(
      sql.type(z.object({ pg_get_constraintdef: z.string() }))`
        SELECT pg_get_constraintdef(oid) FROM pg_constraint
        WHERE conrelid = ${this.toLiteralValue()}::regclass AND contype = 'f'
      `,
    );
    return result.map((row) => row.pg_get_constraintdef);
  }
}
