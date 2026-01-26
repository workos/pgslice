import { CommonQueryMethods, sql, type PrimitiveValueExpression } from "slonik";
import { z } from "zod";
import { Table } from "./table.js";
import type { ColumnInfo, IdValue, SynchronizeBatchResult, SynchronizeOptions } from "./types.js";

/**
 * Transforms a raw ID value from the database into the proper IdValue type.
 * Numbers and numeric strings become bigint, ULID strings stay as strings.
 */
function transformIdValue(val: bigint | number | string): IdValue {
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
 * Converts a value to a SQL fragment using the appropriate slonik helper based on column data type.
 * Uses explicit type information instead of heuristics to properly handle timestamps, dates, etc.
 */
function valueToSql(val: unknown, dataType: string) {
  if (val === null) {
    return sql.fragment`NULL`;
  }

  // Timestamp types - slonik returns as ms since epoch
  if (
    dataType === "timestamp with time zone" ||
    dataType === "timestamp without time zone"
  ) {
    if (typeof val === "number") {
      return sql.timestamp(new Date(val));
    }
    if (val instanceof Date) {
      return sql.timestamp(val);
    }
  }

  // Date type - slonik returns as string "YYYY-MM-DD"
  if (dataType === "date") {
    if (typeof val === "string") {
      return sql.date(new Date(val));
    }
    if (val instanceof Date) {
      return sql.date(val);
    }
  }

  // UUID type
  if (dataType === "uuid" && typeof val === "string") {
    return sql.uuid(val);
  }

  // Binary type
  if (dataType === "bytea" && Buffer.isBuffer(val)) {
    return sql.binary(val);
  }

  // All other types - pass directly to slonik
  return sql.fragment`${val as PrimitiveValueExpression}`;
}

/**
 * Zod schema for a row from the database. All values are nullable.
 */
const rowSchema = z.record(z.string(), z.unknown());

interface SynchronizerOptions {
  source: Table;
  target: Table;
  primaryKeyColumn: string;
  columns: ColumnInfo[];
  windowSize: number;
  startingId: IdValue;
  dryRun: boolean;
}

/**
 * Handles batch synchronization of data between a source table and target table.
 * Detects and fixes discrepancies via INSERT/UPDATE/DELETE operations.
 */
export class Synchronizer {
  readonly #source: Table;
  readonly #target: Table;
  readonly #primaryKeyColumn: string;
  readonly #columns: ColumnInfo[];
  readonly #columnDataTypes: Map<string, string>;
  readonly #windowSize: number;
  readonly #startingId: IdValue;
  readonly #dryRun: boolean;

  private constructor(options: SynchronizerOptions) {
    this.#source = options.source;
    this.#target = options.target;
    this.#primaryKeyColumn = options.primaryKeyColumn;
    this.#columns = options.columns;
    this.#columnDataTypes = new Map(
      options.columns.map((col) => [col.name, col.dataType]),
    );
    this.#windowSize = options.windowSize;
    this.#startingId = options.startingId;
    this.#dryRun = options.dryRun;
  }

  get source(): Table {
    return this.#source;
  }

  get target(): Table {
    return this.#target;
  }

  /**
   * Factory method to create a Synchronizer from SynchronizeOptions.
   * Verifies tables exist, columns match, and determines starting position.
   */
  static async init(
    tx: CommonQueryMethods,
    options: SynchronizeOptions,
  ): Promise<Synchronizer> {
    const table = Table.parse(options.table);
    const sourceTable = table;
    const targetTable = table.intermediate();

    // Verify both tables exist
    if (!(await sourceTable.exists(tx))) {
      throw new Error(`Table not found: ${sourceTable.toString()}`);
    }
    if (!(await targetTable.exists(tx))) {
      throw new Error(`Table not found: ${targetTable.toString()}`);
    }

    // Get columns from both tables (excluding generated columns)
    const sourceColumns = await sourceTable.columns(tx);
    const targetColumns = await targetTable.columns(tx);

    // Verify schemas match
    const sourceColNames = new Set(sourceColumns.map((c) => c.name));
    const targetColNames = new Set(targetColumns.map((c) => c.name));

    for (const col of sourceColumns) {
      if (!targetColNames.has(col.name)) {
        throw new Error(
          `Column '${col.name}' exists in ${sourceTable.toString()} but not in ${targetTable.toString()}`,
        );
      }
    }

    for (const col of targetColumns) {
      if (!sourceColNames.has(col.name)) {
        throw new Error(
          `Column '${col.name}' exists in ${targetTable.toString()} but not in ${sourceTable.toString()}`,
        );
      }
    }

    // Determine primary key
    let primaryKeyColumn: string;
    if (options.primaryKey) {
      primaryKeyColumn = options.primaryKey;
      if (!sourceColNames.has(primaryKeyColumn)) {
        throw new Error(
          `Primary key '${primaryKeyColumn}' not found in source table`,
        );
      }
    } else {
      const pkColumns = await sourceTable.primaryKey(tx);
      if (pkColumns.length === 0) {
        throw new Error(
          "Primary key not found. Specify with --primary-key",
        );
      }
      primaryKeyColumn = pkColumns[0];
    }

    // Determine starting ID
    let startingId: IdValue;
    if (options.start !== undefined) {
      // Parse as bigint if numeric, otherwise keep as string (ULID)
      startingId = /^\d+$/.test(options.start)
        ? BigInt(options.start)
        : options.start;
    } else {
      const minId = await sourceTable.minId(tx, primaryKeyColumn);
      if (minId === null) {
        throw new Error("No rows found in source table");
      }
      startingId = minId;
    }

    return new Synchronizer({
      source: sourceTable,
      target: targetTable,
      primaryKeyColumn,
      columns: sourceColumns,
      windowSize: options.windowSize ?? 1000,
      startingId,
      dryRun: options.dryRun ?? false,
    });
  }

  /**
   * Synchronizes data in batches, yielding results after each batch.
   *
   * @param connection - Database connection pool or query methods
   */
  async *synchronize(
    connection: CommonQueryMethods,
  ): AsyncGenerator<SynchronizeBatchResult> {
    let currentId = this.#startingId;
    let includeStart = true;
    let batchNumber = 0;

    while (true) {
      batchNumber++;
      const batchStartTime = performance.now();

      // Fetch batch from source
      const sourceRows = await this.#fetchBatch(
        connection,
        this.#source,
        currentId,
        includeStart,
      );

      // Stop if no rows returned
      if (sourceRows.length === 0) {
        break;
      }

      const firstPk = sourceRows[0][this.#primaryKeyColumn] as IdValue;
      const lastPk = sourceRows[sourceRows.length - 1][this.#primaryKeyColumn] as IdValue;

      // Fetch target rows in the same range
      const targetRows = await this.#fetchRowsByRange(
        connection,
        this.#target,
        firstPk,
        lastPk,
      );

      // Build lookup map for target rows
      const targetRowsByPk = new Map<string, Record<string, unknown>>();
      for (const row of targetRows) {
        const pk = this.#serializePk(row[this.#primaryKeyColumn] as IdValue);
        targetRowsByPk.set(pk, row);
      }

      // Compare and track differences
      let matchingRows = 0;
      let rowsInserted = 0;
      let rowsUpdated = 0;
      let rowsDeleted = 0;

      const sourcePks = new Set<string>();

      for (const sourceRow of sourceRows) {
        const pk = sourceRow[this.#primaryKeyColumn] as IdValue;
        const pkKey = this.#serializePk(pk);
        sourcePks.add(pkKey);

        const targetRow = targetRowsByPk.get(pkKey);

        if (targetRow === undefined) {
          // Missing in target - INSERT
          rowsInserted++;
          if (!this.#dryRun) {
            await this.#insertRow(connection, sourceRow);
          }
        } else if (this.#rowsDiffer(sourceRow, targetRow)) {
          // Rows differ - UPDATE
          rowsUpdated++;
          if (!this.#dryRun) {
            await this.#updateRow(connection, sourceRow);
          }
        } else {
          // Rows match
          matchingRows++;
        }
      }

      // Check for extra rows in target (in target but not in source)
      for (const [pkKey, _targetRow] of targetRowsByPk) {
        if (!sourcePks.has(pkKey)) {
          // Extra row in target - DELETE
          rowsDeleted++;
          if (!this.#dryRun) {
            const pk = this.#deserializePk(pkKey);
            await this.#deleteRow(connection, pk);
          }
        }
      }

      const batchEndTime = performance.now();

      yield {
        batchNumber,
        batchDurationMs: batchEndTime - batchStartTime,
        primaryKeyRange: {
          start: transformIdValue(firstPk),
          end: transformIdValue(lastPk),
        },
        rowsCompared: sourceRows.length,
        matchingRows,
        rowsInserted,
        rowsUpdated,
        rowsDeleted,
      };

      // After first batch, use exclusive comparison
      includeStart = false;
      currentId = lastPk;

      // Stop if we processed fewer rows than window size (last batch)
      if (sourceRows.length < this.#windowSize) {
        break;
      }
    }
  }

  #serializePk(pk: IdValue): string {
    return typeof pk === "bigint" ? `bigint:${pk.toString()}` : `string:${pk}`;
  }

  #deserializePk(key: string): IdValue {
    if (key.startsWith("bigint:")) {
      return BigInt(key.slice(7));
    }
    return key.slice(7);
  }

  async #fetchBatch(
    connection: CommonQueryMethods,
    table: Table,
    startingId: IdValue,
    includeStart: boolean,
  ): Promise<readonly Record<string, unknown>[]> {
    const pkCol = sql.identifier([this.#primaryKeyColumn]);
    const columnList = sql.join(
      this.#columns.map((col) => sql.identifier([col.name])),
      sql.fragment`, `,
    );

    const operator = includeStart
      ? sql.fragment`>=`
      : sql.fragment`>`;

    const result = await connection.any(
      sql.type(rowSchema)`
        SELECT ${columnList}
        FROM ${table.toSqlIdentifier()}
        WHERE ${pkCol} ${operator} ${startingId}
        ORDER BY ${pkCol}
        LIMIT ${this.#windowSize}
      `,
    );

    return result;
  }

  async #fetchRowsByRange(
    connection: CommonQueryMethods,
    table: Table,
    firstPk: IdValue,
    lastPk: IdValue,
  ): Promise<readonly Record<string, unknown>[]> {
    const pkCol = sql.identifier([this.#primaryKeyColumn]);
    const columnList = sql.join(
      this.#columns.map((col) => sql.identifier([col.name])),
      sql.fragment`, `,
    );

    const result = await connection.any(
      sql.type(rowSchema)`
        SELECT ${columnList}
        FROM ${table.toSqlIdentifier()}
        WHERE ${pkCol} >= ${firstPk} AND ${pkCol} <= ${lastPk}
        ORDER BY ${pkCol}
      `,
    );

    return result;
  }

  #rowsDiffer(
    sourceRow: Record<string, unknown>,
    targetRow: Record<string, unknown>,
  ): boolean {
    for (const col of this.#columns) {
      const sourceVal = sourceRow[col.name];
      const targetVal = targetRow[col.name];

      // Handle BigInt comparison
      if (typeof sourceVal === "bigint" || typeof targetVal === "bigint") {
        if (String(sourceVal) !== String(targetVal)) {
          return true;
        }
        continue;
      }

      // Handle Date comparison
      if (sourceVal instanceof Date && targetVal instanceof Date) {
        if (sourceVal.getTime() !== targetVal.getTime()) {
          return true;
        }
        continue;
      }

      // Default comparison
      if (sourceVal !== targetVal) {
        return true;
      }
    }
    return false;
  }

  async #insertRow(
    connection: CommonQueryMethods,
    row: Record<string, unknown>,
  ): Promise<void> {
    const columnList = sql.join(
      this.#columns.map((col) => sql.identifier([col.name])),
      sql.fragment`, `,
    );
    const valueList = sql.join(
      this.#columns.map((col) =>
        valueToSql(row[col.name], col.dataType),
      ),
      sql.fragment`, `,
    );

    await connection.query(
      sql.type(z.object({}))`
        INSERT INTO ${this.#target.toSqlIdentifier()} (${columnList})
        VALUES (${valueList})
      `,
    );
  }

  async #updateRow(
    connection: CommonQueryMethods,
    row: Record<string, unknown>,
  ): Promise<void> {
    const pkCol = sql.identifier([this.#primaryKeyColumn]);
    const pkValue = row[this.#primaryKeyColumn];

    const setClauses = this.#columns
      .filter((col) => col.name !== this.#primaryKeyColumn)
      .map((col) => {
        const val = row[col.name];
        return sql.fragment`${sql.identifier([col.name])} = ${valueToSql(val, col.dataType)}`;
      });

    if (setClauses.length === 0) {
      return; // Nothing to update besides PK
    }

    const setClause = sql.join(setClauses, sql.fragment`, `);

    await connection.query(
      sql.type(z.object({}))`
        UPDATE ${this.#target.toSqlIdentifier()}
        SET ${setClause}
        WHERE ${pkCol} = ${pkValue as Parameters<typeof sql.fragment>[1]}
      `,
    );
  }

  async #deleteRow(
    connection: CommonQueryMethods,
    pk: IdValue,
  ): Promise<void> {
    const pkCol = sql.identifier([this.#primaryKeyColumn]);

    await connection.query(
      sql.type(z.object({}))`
        DELETE FROM ${this.#target.toSqlIdentifier()}
        WHERE ${pkCol} = ${pk}
      `,
    );
  }
}
