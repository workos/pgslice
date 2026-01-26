import {
  CommonQueryMethods,
  DatabaseTransactionConnection,
  sql,
  IdentifierSqlToken,
} from "slonik";
import { z } from "zod";
import type { Cast, ColumnInfo, IdValue, SequenceInfo } from "./types.js";
import { TableSettings } from "./table-settings.js";
import { formatDateForSql } from "./sql-utils.js";

/**
 * Zod schema for validating ID values from the database.
 * Note: slonik's sql.type() does NOT run zod transforms, so we use
 * a simple schema here and transform manually in the query methods.
 */
const idValueSchema = z.union([z.bigint(), z.number(), z.string()]).nullable();

/**
 * Transforms a raw ID value from the database into the proper IdValue type.
 * Numbers and numeric strings become bigint, ULID strings stay as strings.
 */
function transformIdValue(
  val: bigint | number | string | null,
): IdValue | null {
  if (val === null) {
    return null;
  }
  if (typeof val === "bigint") {
    return val;
  }
  if (typeof val === "number") {
    return BigInt(val);
  }
  // val is string - check if it's a numeric string or ULID
  if (/^\d+$/.test(val)) {
    return BigInt(val);
  }
  return val;
}

/**
 * Derives the appropriate Cast type from a PostgreSQL data type string.
 * Returns null for types that don't require casting for partition operations.
 */
function dataTypeToCast(dataType: string): Cast | null {
  switch (dataType) {
    case "timestamp with time zone":
      return "timestamptz";
    case "timestamp without time zone":
    case "date":
      return "date";
    default:
      return null;
  }
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
  static parse(name: string, defaultSchema: string = "public"): Table {
    if (name.includes(".")) {
      const [schema, tableName] = name.split(".", 2);
      return new Table(schema, tableName);
    }
    return new Table(defaultSchema, name);
  }

  /**
   * Creates the intermediate table derived from this table.
   */
  get intermediate(): Table {
    return new Table(this.schema, `${this.name}_intermediate`);
  }

  /**
   * Creates the retired table derived from this table.
   */
  get retired(): Table {
    return new Table(this.schema, `${this.name}_retired`);
  }

  /**
   * Creates a SQL identifier token for this table.
   */
  get sqlIdentifier(): IdentifierSqlToken {
    return sql.identifier([this.schema, this.name]);
  }

  /**
   * Returns a quoted literal value for use with ::regclass casting.
   * The schema and table names are double-quoted to preserve case sensitivity.
   */
  get regclassLiteral() {
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
  get quoted(): string {
    return `"${this.schema}"."${this.name}"`;
  }

  /**
   * Checks if this table exists in the database.
   */
  async exists(connection: CommonQueryMethods): Promise<boolean> {
    const result = await connection.one(sql.type(
      z.object({ count: z.coerce.number() }),
    )`
      SELECT COUNT(*) FROM pg_catalog.pg_tables
      WHERE schemaname = ${this.schema} AND tablename = ${this.name}
    `);
    return result.count > 0;
  }

  /**
   * Gets column metadata for this table (excluding generated columns).
   */
  async columns(tx: DatabaseTransactionConnection): Promise<ColumnInfo[]> {
    const result = await tx.any(
      sql.type(z.object({ column_name: z.string(), data_type: z.string() }))`
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema = ${this.schema}
          AND table_name = ${this.name}
          AND is_generated = 'NEVER'
      `,
    );
    return result.map((row) => ({
      name: row.column_name,
      dataType: row.data_type,
      cast: dataTypeToCast(row.data_type),
    }));
  }

  /**
   * Gets index definitions for this table (excluding primary key).
   */
  async indexDefs(tx: DatabaseTransactionConnection): Promise<string[]> {
    const result = await tx.any(
      sql.type(z.object({ pg_get_indexdef: z.string() }))`
        SELECT pg_get_indexdef(indexrelid) FROM pg_index
        WHERE indrelid = ${this.regclassLiteral}::regclass AND indisprimary = 'f'
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
        WHERE conrelid = ${this.regclassLiteral}::regclass AND contype = 'f'
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
    connection: CommonQueryMethods,
  ): Promise<TableSettings | null> {
    const result = await connection.maybeOne(
      sql.type(z.object({ comment: z.string().nullable() }))`
        SELECT obj_description(${this.regclassLiteral}::regclass) AS comment
      `,
    );

    if (!result?.comment) {
      return null;
    }

    return TableSettings.parseFromComment(result.comment);
  }

  /**
   * Gets the maximum ID value for the primary key column.
   *
   * @param tx - Database connection
   * @param primaryKeyColumn - The name of the primary key column
   * @param options - Optional filtering options
   * @param options.below - Only consider IDs below this value
   */
  async maxId(
    tx: CommonQueryMethods,
    primaryKeyColumn: string,
    options?: { below?: IdValue },
  ): Promise<IdValue | null> {
    const col = sql.identifier([primaryKeyColumn]);

    let whereClause = sql.fragment`1 = 1`;
    if (options?.below !== undefined) {
      whereClause = sql.fragment`${col} <= ${options.below}`;
    }

    const result = await tx.maybeOne(
      sql.type(z.object({ max_id: idValueSchema }))`
        SELECT MAX(${col}) AS max_id
        FROM ${this.sqlIdentifier}
        WHERE ${whereClause}
      `,
    );

    return transformIdValue(result?.max_id ?? null);
  }

  /**
   * Gets the minimum ID value for the primary key column.
   *
   * @param tx - Database connection
   * @param primaryKeyColumn - The name of the primary key column
   * @param options - Optional time filtering options for partitioned tables
   */
  async minId(
    tx: CommonQueryMethods,
    primaryKeyColumn: string,
    options?: {
      column?: string;
      cast?: Cast;
      startingTime?: Date;
    },
  ): Promise<IdValue | null> {
    const col = sql.identifier([primaryKeyColumn]);

    let whereClause = sql.fragment`1 = 1`;
    if (options?.column && options.cast && options.startingTime) {
      const timeCol = sql.identifier([options.column]);
      const startDate = formatDateForSql(options.startingTime, options.cast);
      whereClause = sql.fragment`${timeCol} >= ${startDate}`;
    }

    const result = await tx.maybeOne(
      sql.type(z.object({ min_id: idValueSchema }))`
        SELECT MIN(${col}) AS min_id
        FROM ${this.sqlIdentifier}
        WHERE ${whereClause}
      `,
    );

    return transformIdValue(result?.min_id ?? null);
  }

  /**
   * Gets sequences attached to this table's columns.
   */
  async sequences(tx: DatabaseTransactionConnection): Promise<SequenceInfo[]> {
    const result = await tx.any(
      sql.type(
        z.object({
          sequence_schema: z.string(),
          sequence_name: z.string(),
          related_column: z.string(),
        }),
      )`
        SELECT
          a.attname AS related_column,
          n.nspname AS sequence_schema,
          s.relname AS sequence_name
        FROM pg_class s
          INNER JOIN pg_depend d ON d.objid = s.oid
          INNER JOIN pg_class t ON d.objid = s.oid AND d.refobjid = t.oid
          INNER JOIN pg_attribute a ON (d.refobjid, d.refobjsubid) = (a.attrelid, a.attnum)
          INNER JOIN pg_namespace n ON n.oid = s.relnamespace
          INNER JOIN pg_namespace nt ON nt.oid = t.relnamespace
        WHERE s.relkind = 'S'
          AND nt.nspname = ${this.schema}
          AND t.relname = ${this.name}
        ORDER BY s.relname ASC
      `,
    );

    return result.map((row) => ({
      sequenceSchema: row.sequence_schema,
      sequenceName: row.sequence_name,
      relatedColumn: row.related_column,
    }));
  }
}
