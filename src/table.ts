import { DatabaseTransactionConnection, sql, IdentifierSqlToken } from "slonik";
import { z } from "zod";
import type { Cast, Period, TableSettings } from "./types.js";
import { PERIODS } from "./types.js";

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
   * Returns a quoted literal value for use with ::regclass casting.
   * The schema and table names are double-quoted to preserve case sensitivity.
   */
  toRegclassLiteral() {
    return sql.literalValue(`"${this.schema}"."${this.name}"`);
  }

  /**
   * Returns the string representation of this table (schema.name).
   */
  toString(): string {
    return `${this.schema}.${this.name}`;
  }

  /**
   * Returns a properly quoted string representation of this table.
   * Both schema and name are double-quoted to preserve case.
   */
  toQuotedString(): string {
    return `"${this.schema}"."${this.name}"`;
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
        WHERE indrelid = ${this.toRegclassLiteral()}::regclass AND indisprimary = 'f'
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
        WHERE conrelid = ${this.toRegclassLiteral()}::regclass AND contype = 'f'
      `,
    );
    return result.map((row) => row.pg_get_constraintdef);
  }

  /**
   * Creates a partition table derived from this table with the given suffix.
   */
  partition(suffix: string): Table {
    return new Table(this.schema, `${this.name}_${suffix}`);
  }

  /**
   * Gets the primary key column names for this table in order.
   */
  async primaryKey(tx: DatabaseTransactionConnection): Promise<string[]> {
    const result = await tx.any(
      sql.type(
        z.object({
          attname: z.string(),
          attnum: z.coerce.string(),
          indkey: z.string(),
        }),
      )`
        SELECT
          pg_attribute.attname,
          pg_attribute.attnum,
          pg_index.indkey
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          nspname = ${this.schema} AND
          relname = ${this.name} AND
          indrelid = pg_class.oid AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      `,
    );

    return [...result]
      .sort((a, b) => {
        const keys = a.indkey.split(" ");
        return keys.indexOf(a.attnum) - keys.indexOf(b.attnum);
      })
      .map((r) => r.attname);
  }

  /**
   * Gets all child partitions of this table.
   */
  async partitions(tx: DatabaseTransactionConnection): Promise<Table[]> {
    const result = await tx.any(
      sql.type(z.object({ schema: z.string(), name: z.string() }))`
        SELECT
          nmsp_child.nspname AS schema,
          child.relname AS name
        FROM pg_inherits
          JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
          JOIN pg_class child ON pg_inherits.inhrelid = child.oid
          JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
          JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
        WHERE
          nmsp_parent.nspname = ${this.schema} AND
          parent.relname = ${this.name}
        ORDER BY child.relname ASC
      `,
    );

    return result.map((r) => new Table(r.schema, r.name));
  }

  /**
   * Fetches the partition settings from this table's comment.
   */
  async fetchSettings(
    tx: DatabaseTransactionConnection,
  ): Promise<TableSettings | null> {
    const result = await tx.maybeOne(
      sql.type(z.object({ comment: z.string().nullable() }))`
        SELECT obj_description(${this.toRegclassLiteral()}::regclass) AS comment
      `,
    );

    if (!result?.comment) {
      return null;
    }

    return parseSettingsComment(result.comment);
  }
}

function parseSettingsComment(comment: string): TableSettings | null {
  const parts = comment.split(",");
  const settings: Partial<TableSettings> = {};

  for (const part of parts) {
    const [key, value] = part.split(":");
    if (key === "column") {
      settings.column = value;
    } else if (key === "period" && isValidPeriod(value)) {
      settings.period = value;
    } else if (key === "cast" && isValidCast(value)) {
      settings.cast = value;
    }
  }

  if (settings.column && settings.period && settings.cast) {
    return settings as TableSettings;
  }

  return null;
}

function isValidPeriod(value: string): value is Period {
  return PERIODS.includes(value as Period);
}

function isValidCast(value: string): value is Cast {
  return value === "date" || value === "timestamptz";
}
