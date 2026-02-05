import { describe, expect, it } from "vitest";
import { formatDateForSql, rawSql } from "./sql-utils.js";

describe("rawSql", () => {
  it("creates a SQL fragment from a raw string", () => {
    const fragment = rawSql("SELECT 1");
    expect(fragment).toBeDefined();
    expect(fragment.sql).toBe("SELECT 1");
  });

  it("preserves the exact query string", () => {
    const query = "CREATE INDEX CONCURRENTLY ON foo (bar)";
    const fragment = rawSql(query);
    expect(fragment.sql).toBe(query);
  });

  it("has empty values array", () => {
    const fragment = rawSql("SELECT * FROM users");
    expect(fragment.values).toEqual([]);
  });
});

describe("formatDateForSql", () => {
  describe("with date cast", () => {
    it("formats a date as YYYY-MM-DD", () => {
      const date = new Date(Date.UTC(2024, 0, 15));
      const result = formatDateForSql(date, "date");

      expect(result.sql).toBe("'2024-01-15'");
    });

    it("pads single-digit months and days", () => {
      const date = new Date(Date.UTC(2024, 2, 5));
      const result = formatDateForSql(date, "date");

      expect(result.sql).toBe("'2024-03-05'");
    });

    it("handles year boundaries", () => {
      const date = new Date(Date.UTC(2023, 11, 31));
      const result = formatDateForSql(date, "date");

      expect(result.sql).toBe("'2023-12-31'");
    });
  });

  describe("with timestamptz cast", () => {
    it("formats a date with time and UTC timezone", () => {
      const date = new Date(Date.UTC(2024, 0, 15));
      const result = formatDateForSql(date, "timestamptz");

      expect(result.sql).toBe("'2024-01-15 00:00:00 UTC'");
    });

    it("pads single-digit months and days", () => {
      const date = new Date(Date.UTC(2024, 2, 5));
      const result = formatDateForSql(date, "timestamptz");

      expect(result.sql).toBe("'2024-03-05 00:00:00 UTC'");
    });

    it("ignores the time component of the input date", () => {
      const date = new Date(Date.UTC(2024, 5, 20, 14, 30, 45));
      const result = formatDateForSql(date, "timestamptz");

      expect(result.sql).toBe("'2024-06-20 00:00:00 UTC'");
    });
  });

  it("uses UTC values regardless of local timezone", () => {
    const date = new Date(Date.UTC(2024, 0, 1, 0, 0, 0));
    const dateResult = formatDateForSql(date, "date");
    const timestampResult = formatDateForSql(date, "timestamptz");

    expect(dateResult.sql).toBe("'2024-01-01'");
    expect(timestampResult.sql).toBe("'2024-01-01 00:00:00 UTC'");
  });
});
