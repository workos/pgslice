import {
  DatabaseTransactionConnection,
  sql,
  IdentifierSqlToken,
} from "slonik";
import { z } from "zod";
import type { Cast, TableRef } from "./types.js";

/**
 * Creates a SQL identifier token for a table reference.
 */
export function sqlTable(table: TableRef): IdentifierSqlToken {
  return sql.identifier([table.schema, table.name]);
}

/**
 * Creates a SQL identifier token for a single identifier (column name, etc.).
 */
export function sqlIdent(name: string): IdentifierSqlToken {
  return sql.identifier([name]);
}

const DEFAULT_SCHEMA = "public";

/**
 * Parses a table name string into a TableRef with schema and name.
 * If no schema is provided, defaults to "public".
 */
export function parseTableName(name: string, defaultSchema?: string): TableRef {
  if (name.includes(".")) {
    const [schema, tableName] = name.split(".", 2);
    return { schema, name: tableName };
  }
  return { schema: defaultSchema ?? DEFAULT_SCHEMA, name };
}

/**
 * Creates the intermediate table reference from a source table.
 */
export function intermediateTable(table: TableRef): TableRef {
  return { schema: table.schema, name: `${table.name}_intermediate` };
}

/**
 * Quotes an identifier for safe use in SQL statements.
 */
function quoteIdent(name: string): string {
  // Escape any double quotes by doubling them
  const escaped = name.replace(/"/g, '""');
  return `"${escaped}"`;
}

/**
 * Quotes a table reference for safe use in SQL statements.
 */
export function quoteTable(table: TableRef): string {
  return `${quoteIdent(table.schema)}.${quoteIdent(table.name)}`;
}

/**
 * Converts a TableRef to its string representation.
 */
export function tableToString(table: TableRef): string {
  return `${table.schema}.${table.name}`;
}

/**
 * Checks if a table exists in the database.
 */
export async function tableExists(
  tx: DatabaseTransactionConnection,
  table: TableRef,
): Promise<boolean> {
  const result = await tx.one(sql.type(z.object({ count: z.coerce.number() }))`
    SELECT COUNT(*) FROM pg_catalog.pg_tables
    WHERE schemaname = ${table.schema} AND tablename = ${table.name}
  `);
  return result.count > 0;
}

/**
 * Gets the list of column names for a table (excluding generated columns).
 */
export async function getColumns(
  tx: DatabaseTransactionConnection,
  table: TableRef,
): Promise<string[]> {
  const result = await tx.any(
    sql.type(z.object({ column_name: z.string() }))`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = ${table.schema}
        AND table_name = ${table.name}
        AND is_generated = 'NEVER'
    `,
  );
  return result.map((row) => row.column_name);
}

/**
 * Gets the cast type for a column (date or timestamptz).
 */
export async function getColumnCast(
  tx: DatabaseTransactionConnection,
  table: TableRef,
  column: string,
): Promise<Cast> {
  const result = await tx.maybeOne(
    sql.type(z.object({ data_type: z.string() }))`
      SELECT data_type FROM information_schema.columns
      WHERE table_schema = ${table.schema}
        AND table_name = ${table.name}
        AND column_name = ${column}
    `,
  );
  if (!result) {
    throw new Error(`Column not found: ${column}`);
  }
  return result.data_type === "timestamp with time zone" ? "timestamptz" : "date";
}

/**
 * Gets index definitions for a table (excluding primary key).
 */
export async function getIndexDefs(
  tx: DatabaseTransactionConnection,
  table: TableRef,
): Promise<string[]> {
  const quotedTable = quoteTable(table);
  const result = await tx.any(
    sql.type(z.object({ pg_get_indexdef: z.string() }))`
      SELECT pg_get_indexdef(indexrelid) FROM pg_index
      WHERE indrelid = ${quotedTable}::regclass AND indisprimary = 'f'
    `,
  );
  return result.map((row) => row.pg_get_indexdef);
}

/**
 * Gets foreign key constraint definitions for a table.
 */
export async function getForeignKeys(
  tx: DatabaseTransactionConnection,
  table: TableRef,
): Promise<string[]> {
  const quotedTable = quoteTable(table);
  const result = await tx.any(
    sql.type(z.object({ pg_get_constraintdef: z.string() }))`
      SELECT pg_get_constraintdef(oid) FROM pg_constraint
      WHERE conrelid = ${quotedTable}::regclass AND contype = 'f'
    `,
  );
  return result.map((row) => row.pg_get_constraintdef);
}

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

