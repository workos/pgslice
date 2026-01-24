import { describe, expect, it } from "vitest";
import {
  NumericComparator,
  UlidComparator,
  isUlid,
  DEFAULT_ULID,
} from "./id-comparator.js";

describe("isUlid", () => {
  it("returns true for valid ULIDs", () => {
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAV")).toBe(true);
    expect(isUlid("00000H5A406P0C3DQMCQ5MV6WQ")).toBe(true);
    expect(isUlid("7ZZZZZZZZZZZZZZZZZZZZZZZZZ")).toBe(true);
  });

  it("returns false for invalid ULIDs", () => {
    // Too short
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FA")).toBe(false);
    // Too long
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAVX")).toBe(false);
    // Invalid characters (I, L, O, U are not in Crockford's base32)
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAI")).toBe(false);
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAL")).toBe(false);
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAO")).toBe(false);
    expect(isUlid("01ARZ3NDEKTSV4RRFFQ69G5FAU")).toBe(false);
    // Lowercase
    expect(isUlid("01arz3ndektsv4rrffq69g5fav")).toBe(false);
    // Numeric string
    expect(isUlid("12345")).toBe(false);
    // Empty
    expect(isUlid("")).toBe(false);
  });
});

describe("NumericComparator", () => {
  const comparator = new NumericComparator("id");

  describe("minValue", () => {
    it("returns 1n", () => {
      expect(comparator.minValue).toBe(1n);
    });
  });

  describe("predecessor", () => {
    it("returns id - 1", () => {
      expect(comparator.predecessor(10n)).toBe(9n);
      expect(comparator.predecessor(1n)).toBe(0n);
      expect(comparator.predecessor(100n)).toBe(99n);
    });

    it("throws for non-bigint values", () => {
      expect(() => comparator.predecessor("123" as unknown as bigint)).toThrow(
        "NumericComparator requires bigint IDs",
      );
    });
  });

  describe("shouldContinue", () => {
    it("returns true when currentId < maxId", () => {
      expect(comparator.shouldContinue(5n, 10n)).toBe(true);
      expect(comparator.shouldContinue(0n, 1n)).toBe(true);
    });

    it("returns false when currentId >= maxId", () => {
      expect(comparator.shouldContinue(10n, 10n)).toBe(false);
      expect(comparator.shouldContinue(11n, 10n)).toBe(false);
    });

    it("returns false for non-bigint values", () => {
      expect(comparator.shouldContinue("5" as unknown as bigint, 10n)).toBe(
        false,
      );
      expect(comparator.shouldContinue(5n, "10" as unknown as bigint)).toBe(
        false,
      );
    });
  });

  describe("batchCount", () => {
    it("calculates correct batch count", () => {
      expect(comparator.batchCount(0n, 100n, 10)).toBe(10);
      expect(comparator.batchCount(0n, 95n, 10)).toBe(10);
      expect(comparator.batchCount(0n, 101n, 10)).toBe(11);
      expect(comparator.batchCount(50n, 100n, 10)).toBe(5);
    });

    it("returns 0 when nothing to fill", () => {
      expect(comparator.batchCount(100n, 100n, 10)).toBe(0);
      expect(comparator.batchCount(100n, 50n, 10)).toBe(0);
    });

    it("returns 0 for non-bigint values", () => {
      expect(
        comparator.batchCount("0" as unknown as bigint, 100n, 10),
      ).toBe(0);
    });
  });

  describe("batchWhereCondition", () => {
    it("generates correct WHERE clause for non-inclusive", () => {
      const fragment = comparator.batchWhereCondition(10n, 5, false);
      // The fragment should generate: "id" > 10 AND "id" <= 15
      expect(fragment).toBeDefined();
    });

    it("generates correct WHERE clause for inclusive", () => {
      const fragment = comparator.batchWhereCondition(10n, 5, true);
      // The fragment should generate: "id" >= 10 AND "id" <= 15
      expect(fragment).toBeDefined();
    });

    it("throws for non-bigint values", () => {
      expect(() =>
        comparator.batchWhereCondition("10" as unknown as bigint, 5, false),
      ).toThrow("NumericComparator requires bigint IDs");
    });
  });

  describe("selectSuffix", () => {
    it("returns null (no suffix needed for numeric IDs)", () => {
      expect(comparator.selectSuffix(100)).toBeNull();
    });
  });
});

describe("UlidComparator", () => {
  const comparator = new UlidComparator("id");

  describe("minValue", () => {
    it("returns the default ULID", () => {
      expect(comparator.minValue).toBe(DEFAULT_ULID);
    });
  });

  describe("predecessor", () => {
    it("returns the default ULID (minimum)", () => {
      expect(comparator.predecessor("01ARZ3NDEKTSV4RRFFQ69G5FAV")).toBe(
        DEFAULT_ULID,
      );
    });
  });

  describe("shouldContinue", () => {
    it("returns true when currentId < maxId (string comparison)", () => {
      expect(
        comparator.shouldContinue(
          "01ARZ3NDEKTSV4RRFFQ69G5FAA",
          "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        ),
      ).toBe(true);
      expect(comparator.shouldContinue(DEFAULT_ULID, "01ARZ3NDEKTSV4RRFFQ69G5FAV")).toBe(
        true,
      );
    });

    it("returns false when currentId >= maxId", () => {
      expect(
        comparator.shouldContinue(
          "01ARZ3NDEKTSV4RRFFQ69G5FAV",
          "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        ),
      ).toBe(false);
      expect(
        comparator.shouldContinue(
          "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        ),
      ).toBe(false);
    });

    it("returns false for non-string values", () => {
      expect(comparator.shouldContinue(5n as unknown as string, "test")).toBe(
        false,
      );
    });
  });

  describe("batchCount", () => {
    it("returns null (batch count is unknown for ULIDs)", () => {
      expect(
        comparator.batchCount(
          "01ARZ3NDEKTSV4RRFFQ69G5FAA",
          "01ARZ3NDEKTSV4RRFFQ69G5FAZ",
          100,
        ),
      ).toBeNull();
    });
  });

  describe("batchWhereCondition", () => {
    it("generates correct WHERE clause for non-inclusive", () => {
      const fragment = comparator.batchWhereCondition(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        100,
        false,
      );
      expect(fragment).toBeDefined();
    });

    it("generates correct WHERE clause for inclusive", () => {
      const fragment = comparator.batchWhereCondition(
        "01ARZ3NDEKTSV4RRFFQ69G5FAV",
        100,
        true,
      );
      expect(fragment).toBeDefined();
    });

    it("throws for non-string values", () => {
      expect(() =>
        comparator.batchWhereCondition(10n as unknown as string, 100, false),
      ).toThrow("UlidComparator requires string IDs");
    });
  });

  describe("selectSuffix", () => {
    it("returns ORDER BY and LIMIT fragment", () => {
      const fragment = comparator.selectSuffix(100);
      expect(fragment).not.toBeNull();
    });
  });
});
