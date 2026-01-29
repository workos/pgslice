import { describe, expect } from "vitest";
import { sql } from "slonik";

import { pgsliceTest as test } from "./testing/index.js";
import { Table } from "./table.js";

describe("Table.maxId", () => {
  test("returns max bigint ID from table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (name) VALUES ('a'), ('b'), ('c')
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction, "id");

    expect(maxId).toBe(3n);
  });

  test("returns null for empty table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction, "id");

    expect(maxId).toBeNull();
  });

  test("returns max string ID from table with ULID", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (id, name) VALUES
        ('01ARZ3NDEKTSV4RRFFQ69G5FAA', 'a'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'b'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAM', 'c')
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction, "id");

    expect(maxId).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAV");
  });

  test("respects below option for numeric IDs", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (name) VALUES ('a'), ('b'), ('c'), ('d'), ('e')
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction, "id", { below: 3n });

    expect(maxId).toBe(3n);
  });

  test("respects below option for string IDs", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (id, name) VALUES
        ('01ARZ3NDEKTSV4RRFFQ69G5FAA', 'a'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAM', 'b'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'c')
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction, "id", {
      below: "01ARZ3NDEKTSV4RRFFQ69G5FAM",
    });

    expect(maxId).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAM");
  });
});

describe("Table.minId", () => {
  test("returns min bigint ID from table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (name) VALUES ('a'), ('b'), ('c')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, "id");

    expect(minId).toBe(1n);
  });

  test("returns null for empty table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, "id");

    expect(minId).toBeNull();
  });

  test("returns min string ID from table with ULID", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (id, name) VALUES
        ('01ARZ3NDEKTSV4RRFFQ69G5FAA', 'a'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'b'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAM', 'c')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, "id");

    expect(minId).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAA");
  });

  test("respects time filter for date column", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        created_at DATE NOT NULL,
        name TEXT
      )
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (created_at, name) VALUES
        ('2024-01-01', 'old'),
        ('2024-06-01', 'mid'),
        ('2024-12-01', 'new')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, "id", {
      column: "created_at",
      cast: "date",
      startingTime: new Date("2024-06-01"),
    });

    expect(minId).toBe(2n);
  });
});

describe("Table.columns", () => {
  test("returns column info with name and data type", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        name TEXT,
        created_at TIMESTAMPTZ
      )
    `);

    const table = Table.parse("test_table");
    const columns = await table.columns(transaction);

    expect(columns).toEqual(
      expect.arrayContaining([
        { name: "id", dataType: "bigint", cast: null },
        { name: "name", dataType: "text", cast: null },
        {
          name: "created_at",
          dataType: "timestamp with time zone",
          cast: "timestamptz",
        },
      ]),
    );
  });

  test("returns proper cast for timestamp types", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        date_col DATE,
        ts_col TIMESTAMP,
        tstz_col TIMESTAMPTZ
      )
    `);

    const table = Table.parse("test_table");
    const columns = await table.columns(transaction);

    const dateCol = columns.find((c) => c.name === "date_col");
    const tsCol = columns.find((c) => c.name === "ts_col");
    const tstzCol = columns.find((c) => c.name === "tstz_col");

    expect(dateCol?.cast).toBe("date");
    expect(tsCol?.cast).toBe("date");
    expect(tstzCol?.cast).toBe("timestamptz");
  });

  test("excludes generated columns", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        name TEXT,
        upper_name TEXT GENERATED ALWAYS AS (UPPER(name)) STORED
      )
    `);

    const table = Table.parse("test_table");
    const columns = await table.columns(transaction);

    expect(columns.map((c) => c.name)).toEqual(["id", "name"]);
  });
});

describe("Table.sequences", () => {
  test("returns sequences attached to SERIAL columns", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name TEXT
      )
    `);

    const table = Table.parse("test_table");
    const sequences = await table.sequences(transaction);

    expect(sequences).toHaveLength(1);
    expect(sequences[0]).toEqual({
      sequenceSchema: "public",
      sequenceName: "test_table_id_seq",
      relatedColumn: "id",
    });
  });

  test("returns sequences attached to BIGSERIAL columns", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        name TEXT
      )
    `);

    const table = Table.parse("test_table");
    const sequences = await table.sequences(transaction);

    expect(sequences).toHaveLength(1);
    expect(sequences[0]).toEqual({
      sequenceSchema: "public",
      sequenceName: "test_table_id_seq",
      relatedColumn: "id",
    });
  });

  test("returns empty array when table has no sequences", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT
      )
    `);

    const table = Table.parse("test_table");
    const sequences = await table.sequences(transaction);

    expect(sequences).toEqual([]);
  });

  test("returns multiple sequences for multiple SERIAL columns", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        secondary_id SERIAL,
        name TEXT
      )
    `);

    const table = Table.parse("test_table");
    const sequences = await table.sequences(transaction);

    expect(sequences).toHaveLength(2);
    expect(sequences.map((s) => s.relatedColumn).sort()).toEqual([
      "id",
      "secondary_id",
    ]);
  });

  test("works with schema-qualified tables", async ({ transaction }) => {
    await transaction.query(sql.unsafe`CREATE SCHEMA test_schema`);
    await transaction.query(sql.unsafe`
      CREATE TABLE test_schema.test_table (
        id SERIAL PRIMARY KEY,
        name TEXT
      )
    `);

    const table = Table.parse("test_schema.test_table");
    const sequences = await table.sequences(transaction);

    expect(sequences).toHaveLength(1);
    expect(sequences[0]).toEqual({
      sequenceSchema: "test_schema",
      sequenceName: "test_table_id_seq",
      relatedColumn: "id",
    });
  });
});

describe("Table.primaryKey", () => {
  test("returns single column primary key", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual(["id"]);
  });

  test("returns composite primary key in correct order", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        tenant_id BIGINT NOT NULL,
        id BIGINT NOT NULL,
        name TEXT,
        PRIMARY KEY (tenant_id, id)
      )
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual(["tenant_id", "id"]);
  });

  test("falls back to id column when no primary key constraint exists", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGINT, name TEXT)
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual(["id"]);
  });

  test("returns empty array when no primary key and no fallback columns", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (user_id BIGINT, name TEXT)
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual([]);
  });

  test("fallback respects primaryKeyFallback static property", async ({
    transaction,
  }) => {
    const originalFallback = Table.primaryKeyFallback;

    try {
      Table.primaryKeyFallback = ["user_id"];

      await transaction.query(sql.unsafe`
        CREATE TABLE test_table (user_id BIGINT, name TEXT)
      `);

      const table = Table.parse("test_table");
      const primaryKey = await table.primaryKey(transaction);

      expect(primaryKey).toEqual(["user_id"]);
    } finally {
      Table.primaryKeyFallback = originalFallback;
    }
  });

  test("fallback is case-insensitive", async ({ transaction }) => {
    // Column "Id" (capital I) should match fallback "id" (lowercase)
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table ("Id" BIGINT, name TEXT)
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual(["Id"]);
  });
});
