import { describe, expect } from "vitest";
import { sql } from "slonik";

import { pgsliceTest as test } from "./testing/index.js";
import { Table } from "./table.js";
import { NumericComparator, UlidComparator } from "./id-comparator.js";

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

  test("returns max string ID from table with ULID", async ({ transaction }) => {
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

  test("returns min string ID from table with ULID", async ({ transaction }) => {
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

describe("Table.createIdComparator", () => {
  test("returns NumericComparator for bigint IDs", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (name) VALUES ('a')
    `);

    const table = Table.parse("test_table");
    const comparator = await table.createIdComparator(transaction, "id");

    expect(comparator).toBeInstanceOf(NumericComparator);
  });

  test("returns UlidComparator for ULID string IDs", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (id, name) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'a')
    `);

    const table = Table.parse("test_table");
    const comparator = await table.createIdComparator(transaction, "id");

    expect(comparator).toBeInstanceOf(UlidComparator);
  });

  test("returns NumericComparator for empty table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const comparator = await table.createIdComparator(transaction, "id");

    expect(comparator).toBeInstanceOf(NumericComparator);
  });

  test("uses hint to determine comparator type for numeric hint", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id TEXT PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const comparator = await table.createIdComparator(
      transaction,
      "id",
      "12345",
    );

    expect(comparator).toBeInstanceOf(NumericComparator);
  });

  test("uses hint to determine comparator type for ULID hint", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const comparator = await table.createIdComparator(
      transaction,
      "id",
      "01ARZ3NDEKTSV4RRFFQ69G5FAV",
    );

    expect(comparator).toBeInstanceOf(UlidComparator);
  });
});
