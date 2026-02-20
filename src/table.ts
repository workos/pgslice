import {
  CommonQueryMethods,
  DatabaseTransactionConnection,
  sql,
  IdentifierSqlToken,
} from "slonik";
import { z } from "zod";
import { advanceDate, parsePartitionDate } from "./date-ranges.js";
import type {
  Cast,
  ColumnInfo,
  IdValue,
  SequenceInfo,
  TimeFilter,
} from "./types.js";
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
export function transformIdValue(val: bigint | number | string): IdValue;
export function transformIdValue(
  val: bigint | number | string | null,
): IdValue | null;
export function transformIdValue(
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

function derivePartitionTimeFilter(
  settings: TableSettings,
  partitions: Table[],
): TimeFilter | undefined {
  if (partitions.length === 0) {
    return undefined;
  }

  const firstPartition = partitions[0];
  const lastPartition = partitions[partitions.length - 1];
  const startingTime = parsePartitionDate(firstPartition.name, settings.period);
  const lastPartitionDate = parsePartitionDate(
    lastPartition.name,
    settings.period,
  );
  const endingTime = advanceDate(lastPartitionDate, settings.period, 1);

  return {
    column: settings.column,
    cast: settings.cast,
    startingTime,
    endingTime,
  };
}

export interface PartitionContext {
  settings: TableSettings | null;
  partitions: Table[];
  timeFilter?: TimeFilter;
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
  /**
   * Fallback primary key column names if none are found in the database.
   */
  static primaryKeyFallback = ["id"];

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
   * Checks if this table is a partitioned table (relkind = 'p').
   */
  async isPartitioned(connection: CommonQueryMethods): Promise<boolean> {
    const result = await connection.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*) FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = ${this.schema}
          AND c.relname = ${this.name}
          AND c.relkind = 'p'
      `,
    );
    return result.count > 0;
  }

  /**
   * Checks if a trigger with the given name exists on this table.
   */
  async triggerExists(
    connection: CommonQueryMethods,
    triggerName: string,
  ): Promise<boolean> {
    const result = await connection.one(
      sql.type(z.object({ count: z.coerce.number() }))`
        SELECT COUNT(*) FROM pg_trigger t
        JOIN pg_class c ON t.tgrelid = c.oid
        JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE n.nspname = ${this.schema}
          AND c.relname = ${this.name}
          AND t.tgname = ${triggerName}
      `,
    );
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

  #primaryKey: string | null = null;

  /**
   * Gets the primary key column names for this table in order.
   */
  async primaryKey(tx: DatabaseTransactionConnection): Promise<string> {
    if (this.#primaryKey) {
      return this.#primaryKey;
    }

    const explicitPrimaryKeys = await tx.any(
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

    switch (explicitPrimaryKeys.length) {
      case 0:
        break; // No explicit primary key found
      case 1:
        this.#primaryKey = explicitPrimaryKeys[0].attname;
        return this.#primaryKey;
      default:
        throw new Error(
          `Composite primary key found (${explicitPrimaryKeys
            .map((pk) => pk.attname)
            .join(", ")}). Not currently supported.`,
        );
    }

    const implicitPrimaryKeys = (await this.columns(tx))
      .map((col) => col.name)
      .filter((name) => Table.primaryKeyFallback.includes(name.toLowerCase()));

    if (implicitPrimaryKeys.length === 1) {
      this.#primaryKey = implicitPrimaryKeys[0];
      return this.#primaryKey;
    }

    throw new Error(`Primary key not found in "${this.toString()}".`);
  }

  /**
   * Gets all child partitions of this table.
   */
  async partitions(tx: CommonQueryMethods): Promise<Table[]> {
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
    options?: { below?: IdValue },
  ): Promise<IdValue | null> {
    const primaryKeyColumn = sql.identifier([await this.primaryKey(tx)]);

    let whereClause = sql.fragment`1 = 1`;
    if (options?.below !== undefined) {
      whereClause = sql.fragment`${primaryKeyColumn} <= ${options.below}`;
    }

    const result = await tx.maybeOne(
      sql.type(z.object({ max_id: idValueSchema }))`
        SELECT MAX(${primaryKeyColumn}) AS max_id
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
    options?: {
      column?: string;
      cast?: Cast;
      startingTime?: Date;
    },
  ): Promise<IdValue | null> {
    const col = sql.identifier([await this.primaryKey(tx)]);

    let whereClause = sql.fragment`1 = 1`;

    if (options?.column && options.cast && options.startingTime) {
      const timeCol = sql.identifier([options.column]);
      const startDate = formatDateForSql(options.startingTime, options.cast);

      whereClause = sql.fragment`${timeCol} >= ${startDate}`;
    }

    // We want the smallest PK within the time range, so we order by PK and take
    // the first row. Ordering by time (created_at) is faster, but only correct
    // if PK order is monotonic with the partition column. With ULIDs, clock
    // skew, backfills, or manual timestamps can yield smaller PKs with later
    // timestamps, which would be skipped. This path favors correctness; it can
    // be slower on large tables without a supporting index.
    const result = await tx.maybeOne(
      sql.type(z.object({ min_id: idValueSchema }))`
        SELECT ${col} AS min_id
        FROM ${this.sqlIdentifier}
        WHERE ${whereClause}
        ORDER BY ${col} ASC
        LIMIT 1
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

  async partitionContext(tx: CommonQueryMethods): Promise<PartitionContext> {
    const settings = await this.fetchSettings(tx);
    if (!settings) {
      return { settings: null, partitions: [], timeFilter: undefined };
    }

    const partitions = await this.partitions(tx);
    const timeFilter = derivePartitionTimeFilter(settings, partitions);

    return { settings, partitions, timeFilter };
  }

  async partitionTimeFilter(
    tx: CommonQueryMethods,
  ): Promise<TimeFilter | undefined> {
    const { timeFilter } = await this.partitionContext(tx);
    return timeFilter;
  }
}
