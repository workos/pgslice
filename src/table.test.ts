import { describe, expect } from "vitest";
import { sql } from "slonik";

import { pgsliceTest as test } from "./testing/index.js";
import { Table } from "./table.js";
import {
  addChild,
  addDefault,
  addMinvalueChild,
  createCdcRole,
  nativeParent,
  TS,
  TSTZ,
} from "./testing/shapes.js";

describe("Table.isPartitioned", () => {
  test("returns true for partitioned table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL,
        created_at DATE NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);

    const table = Table.parse("test_table");
    const result = await table.isPartitioned(transaction);

    expect(result).toBe(true);
  });

  test("returns false for regular table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const result = await table.isPartitioned(transaction);

    expect(result).toBe(false);
  });

  test("returns false for non-existent table", async ({ transaction }) => {
    const table = Table.parse("nonexistent_table");
    const result = await table.isPartitioned(transaction);

    expect(result).toBe(false);
  });
});

describe("Table.triggerExists", () => {
  test("returns true when trigger exists", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      CREATE FUNCTION test_trigger_fn() RETURNS TRIGGER AS $$
      BEGIN RETURN NEW; END;
      $$ LANGUAGE plpgsql
    `);
    await transaction.query(sql.unsafe`
      CREATE TRIGGER test_trigger
      AFTER INSERT ON test_table
      FOR EACH ROW EXECUTE FUNCTION test_trigger_fn()
    `);

    const table = Table.parse("test_table");
    const result = await table.triggerExists(transaction, "test_trigger");

    expect(result).toBe(true);
  });

  test("returns false when trigger does not exist", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const result = await table.triggerExists(
      transaction,
      "nonexistent_trigger",
    );

    expect(result).toBe(false);
  });

  test("returns false for trigger on different table", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE other_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      CREATE FUNCTION test_trigger_fn() RETURNS TRIGGER AS $$
      BEGIN RETURN NEW; END;
      $$ LANGUAGE plpgsql
    `);
    await transaction.query(sql.unsafe`
      CREATE TRIGGER test_trigger
      AFTER INSERT ON other_table
      FOR EACH ROW EXECUTE FUNCTION test_trigger_fn()
    `);

    const table = Table.parse("test_table");
    const result = await table.triggerExists(transaction, "test_trigger");

    expect(result).toBe(false);
  });
});

describe("Table.maxId", () => {
  test("returns max bigint ID from table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (name) VALUES ('a'), ('b'), ('c')
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction);

    expect(maxId).toBe(3n);
  });

  test("returns null for empty table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const maxId = await table.maxId(transaction);

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
    const maxId = await table.maxId(transaction);

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
    const maxId = await table.maxId(transaction, { below: 3n });

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
    const maxId = await table.maxId(transaction, {
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
    const minId = await table.minId(transaction);

    expect(minId).toBe(1n);
  });

  test("returns null for empty table", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGSERIAL PRIMARY KEY, name TEXT)
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction);

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
    const minId = await table.minId(transaction);

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
    const minId = await table.minId(transaction, {
      column: "created_at",
      cast: "date",
      startingTime: new Date("2024-06-01"),
    });

    expect(minId).toBe(2n);
  });

  test("respects time filter for timestamptz column", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL,
        name TEXT
      )
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (created_at, name) VALUES
        ('2024-01-01 00:00:00 UTC', 'old'),
        ('2024-06-01 00:00:00 UTC', 'mid'),
        ('2024-12-01 00:00:00 UTC', 'new')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, {
      column: "created_at",
      cast: "timestamptz",
      startingTime: new Date("2024-06-01T00:00:00Z"),
    });

    expect(minId).toBe(2n);
  });

  test("returns null when time filter excludes all rows", async ({
    transaction,
  }) => {
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
        ('2024-03-01', 'mid'),
        ('2024-06-01', 'new')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, {
      column: "created_at",
      cast: "date",
      startingTime: new Date("2025-01-01"),
    });

    expect(minId).toBeNull();
  });

  test("respects time filter with string IDs", async ({ transaction }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (
        id TEXT PRIMARY KEY,
        created_at DATE NOT NULL,
        name TEXT
      )
    `);
    await transaction.query(sql.unsafe`
      INSERT INTO test_table (id, created_at, name) VALUES
        ('01ARZ3NDEKTSV4RRFFQ69G5FAA', '2024-01-01', 'old'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAM', '2024-06-01', 'mid'),
        ('01ARZ3NDEKTSV4RRFFQ69G5FAV', '2024-12-01', 'new')
    `);

    const table = Table.parse("test_table");
    const minId = await table.minId(transaction, {
      column: "created_at",
      cast: "date",
      startingTime: new Date("2024-06-01"),
    });

    expect(minId).toBe("01ARZ3NDEKTSV4RRFFQ69G5FAM");
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

    expect(primaryKey).toEqual("id");
  });

  test("falls back to id column when no primary key constraint exists", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (id BIGINT, name TEXT)
    `);

    const table = Table.parse("test_table");
    const primaryKey = await table.primaryKey(transaction);

    expect(primaryKey).toEqual("id");
  });

  test("throws an error wheh nno primary key and no fallback columns", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE test_table (user_id BIGINT, name TEXT)
    `);

    const table = Table.parse("test_table");

    await expect(table.primaryKey(transaction)).rejects.toThrow(
      'Primary key not found in "public.test_table".',
    );
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

      expect(primaryKey).toEqual("user_id");
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

    expect(primaryKey).toEqual("Id");
  });
});

describe("Table.primaryKeyColumns", () => {
  test("returns a composite primary key in key order", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE composite_pk (
        id bigint NOT NULL,
        created_at timestamp NOT NULL,
        PRIMARY KEY (id, created_at)
      )
    `);
    expect(
      await Table.parse("composite_pk").primaryKeyColumns(transaction),
    ).toEqual(["id", "created_at"]);
  });

  test("returns a three-column composite key in declared order", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE three_pk (
        tenant_id bigint NOT NULL,
        id bigint NOT NULL,
        created_at timestamp NOT NULL,
        PRIMARY KEY (tenant_id, id, created_at)
      )
    `);
    expect(
      await Table.parse("three_pk").primaryKeyColumns(transaction),
    ).toEqual(["tenant_id", "id", "created_at"]);
  });

  test("returns an empty array when there is no primary key", async ({
    transaction,
  }) => {
    await transaction.query(
      sql.unsafe`CREATE TABLE no_pk (id bigint, payload text)`,
    );
    expect(await Table.parse("no_pk").primaryKeyColumns(transaction)).toEqual(
      [],
    );
  });
});

describe("Table.grants", () => {
  test("returns each grantee's privileges with grantable flags, excluding the owner", async ({
    transaction,
  }) => {
    await createCdcRole(transaction, "cdc_table_grants");
    await transaction.query(
      sql.unsafe`CREATE TABLE granted (id bigint, created_at timestamptz)`,
    );
    await transaction.query(sql.unsafe`
      GRANT SELECT ON granted TO cdc_table_grants WITH GRANT OPTION
    `);
    await transaction.query(
      sql.unsafe`GRANT INSERT ON granted TO cdc_table_grants`,
    );

    const grants = await Table.parse("granted").grants(transaction);

    expect(grants).toEqual(
      expect.arrayContaining([
        { grantee: "cdc_table_grants", privilege: "SELECT", grantable: true },
        { grantee: "cdc_table_grants", privilege: "INSERT", grantable: false },
      ]),
    );
    // The owner's implicit privileges are excluded.
    expect(grants.every((g) => g.grantee !== "postgres")).toBe(true);
  });

  test("represents a PUBLIC grant with a null grantee", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`CREATE TABLE public_grant (id bigint)`);
    await transaction.query(sql.unsafe`GRANT SELECT ON public_grant TO PUBLIC`);

    const grants = await Table.parse("public_grant").grants(transaction);

    expect(grants).toContainEqual({
      grantee: null,
      privilege: "SELECT",
      grantable: false,
    });
  });
});

describe("Table.rangePartitionBounds", () => {
  test("parses a mixed set of finite, MINVALUE, and DEFAULT bounds", async ({
    transaction,
  }) => {
    await nativeParent(
      transaction,
      "bounds",
      "created_at",
      TS,
      "month",
      "date",
    );
    await addChild(
      transaction,
      "bounds",
      "bounds_202601",
      "2026-01-01",
      "2026-02-01",
    );
    await addMinvalueChild(
      transaction,
      "bounds",
      "bounds_historic",
      "2025-01-01",
    );
    await addDefault(transaction, "bounds", "bounds_default");

    const ranges =
      await Table.parse("bounds").rangePartitionBounds(transaction);

    expect(ranges).toHaveLength(3);
    expect(ranges.filter((r) => r.isDefault)).toHaveLength(1);

    const historic = ranges.find((r) => r.lowerUnbounded && !r.isDefault);
    expect(historic?.upper).toEqual(new Date("2025-01-01T00:00:00Z"));

    const finiteRange = ranges.find(
      (r) => !r.isDefault && !r.lowerUnbounded && !r.upperUnbounded,
    );
    expect(finiteRange?.lower).toEqual(new Date("2026-01-01T00:00:00Z"));
    expect(finiteRange?.upper).toEqual(new Date("2026-02-01T00:00:00Z"));
  });

  test("parses timestamptz bounds as UTC under a UTC-pinned session", async ({
    transaction,
  }) => {
    // The documented contract: read bounds in a UTC-pinned transaction.
    await transaction.query(sql.unsafe`SET LOCAL TIME ZONE 'UTC'`);
    await nativeParent(
      transaction,
      "tz_bounds",
      "created_at",
      TSTZ,
      "week",
      "timestamptz",
    );
    await addChild(
      transaction,
      "tz_bounds",
      "tz_bounds_2026w03",
      "2026-01-12 00:00:00+00",
      "2026-01-19 00:00:00+00",
    );

    const ranges =
      await Table.parse("tz_bounds").rangePartitionBounds(transaction);
    const finiteRange = ranges.find((r) => !r.isDefault);
    expect(finiteRange?.lower).toEqual(new Date("2026-01-12T00:00:00Z"));
    expect(finiteRange?.upper).toEqual(new Date("2026-01-19T00:00:00Z"));
  });
});

describe("Table.unsafeReplicaIdentityPartitions", () => {
  test("flags an unsafe leaf beneath a sub-partitioned child", async ({
    transaction,
  }) => {
    await transaction.query(sql.unsafe`
      CREATE TABLE nested_parent (
        id bigint NOT NULL,
        created_at timestamp NOT NULL,
        PRIMARY KEY (id, created_at)
      ) PARTITION BY RANGE (created_at)
    `);
    // A direct child that is itself partitioned (multi-level partitioning).
    await transaction.query(sql.unsafe`
      CREATE TABLE nested_parent_2026 PARTITION OF nested_parent
        FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')
        PARTITION BY RANGE (created_at)
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE nested_parent_2026m01 PARTITION OF nested_parent_2026
        FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')
    `);
    await transaction.query(sql.unsafe`
      CREATE TABLE nested_parent_2026m02 PARTITION OF nested_parent_2026
        FOR VALUES FROM ('2026-02-01') TO ('2026-03-01')
    `);
    await transaction.query(sql.unsafe`
      ALTER TABLE nested_parent_2026m02 REPLICA IDENTITY NOTHING
    `);

    const unsafe =
      await Table.parse("nested_parent").unsafeReplicaIdentityPartitions(
        transaction,
      );

    // Only true leaves are inspected. The unsafe nested leaf is caught; a
    // direct-children-only check would see the (safe) sub-partitioned child and
    // miss it.
    expect(unsafe).toEqual(["nested_parent_2026m02"]);
  });
});
