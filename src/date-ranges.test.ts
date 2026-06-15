import { describe, expect, it } from "vitest";
import {
  DateRanges,
  advanceDate,
  formatDateSuffix,
  parsePartitionDate,
  roundDate,
} from "./date-ranges.js";

describe("roundDate", () => {
  it("rounds to start of day", () => {
    const date = new Date(Date.UTC(2026, 0, 21, 15, 30, 45));
    const result = roundDate(date, "day");
    expect(result).toEqual(new Date(Date.UTC(2026, 0, 21)));
  });

  it("rounds to start of ISO week (Monday)", () => {
    // 2026-08-15 is a Saturday; its ISO week starts Monday 2026-08-10.
    const date = new Date(Date.UTC(2026, 7, 15, 15, 30, 45));
    const result = roundDate(date, "week");
    expect(result).toEqual(new Date(Date.UTC(2026, 7, 10)));
  });

  it("rounds to start of ISO week across a year boundary", () => {
    // 2026-01-01 (Thu) belongs to ISO week 2026-W01, starting 2025-12-29.
    const date = new Date(Date.UTC(2026, 0, 1, 9, 0, 0));
    const result = roundDate(date, "week");
    expect(result).toEqual(new Date(Date.UTC(2025, 11, 29)));
  });

  it("rounds to start of month", () => {
    const date = new Date(Date.UTC(2026, 0, 21, 15, 30, 45));
    const result = roundDate(date, "month");
    expect(result).toEqual(new Date(Date.UTC(2026, 0, 1)));
  });

  it("rounds to start of year", () => {
    const date = new Date(Date.UTC(2026, 5, 21, 15, 30, 45));
    const result = roundDate(date, "year");
    expect(result).toEqual(new Date(Date.UTC(2026, 0, 1)));
  });
});

describe("advanceDate", () => {
  describe("day period", () => {
    it("advances by positive count", () => {
      const date = new Date(Date.UTC(2026, 0, 21));
      const result = advanceDate(date, "day", 5);
      expect(result).toEqual(new Date(Date.UTC(2026, 0, 26)));
    });

    it("advances by negative count", () => {
      const date = new Date(Date.UTC(2026, 0, 21));
      const result = advanceDate(date, "day", -5);
      expect(result).toEqual(new Date(Date.UTC(2026, 0, 16)));
    });

    it("handles month boundary", () => {
      const date = new Date(Date.UTC(2026, 0, 31));
      const result = advanceDate(date, "day", 1);
      expect(result).toEqual(new Date(Date.UTC(2026, 1, 1)));
    });
  });

  describe("week period", () => {
    it("advances by positive count", () => {
      const date = new Date(Date.UTC(2026, 7, 10));
      const result = advanceDate(date, "week", 2);
      expect(result).toEqual(new Date(Date.UTC(2026, 7, 24)));
    });

    it("advances by negative count across a month boundary", () => {
      const date = new Date(Date.UTC(2026, 7, 10));
      const result = advanceDate(date, "week", -2);
      expect(result).toEqual(new Date(Date.UTC(2026, 6, 27)));
    });
  });

  describe("month period", () => {
    it("advances by positive count", () => {
      const date = new Date(Date.UTC(2026, 0, 1));
      const result = advanceDate(date, "month", 3);
      expect(result).toEqual(new Date(Date.UTC(2026, 3, 1)));
    });

    it("advances by negative count", () => {
      const date = new Date(Date.UTC(2026, 5, 1));
      const result = advanceDate(date, "month", -3);
      expect(result).toEqual(new Date(Date.UTC(2026, 2, 1)));
    });

    it("handles year boundary", () => {
      const date = new Date(Date.UTC(2026, 11, 1));
      const result = advanceDate(date, "month", 2);
      expect(result).toEqual(new Date(Date.UTC(2027, 1, 1)));
    });
  });

  describe("year period", () => {
    it("advances by positive count", () => {
      const date = new Date(Date.UTC(2026, 0, 1));
      const result = advanceDate(date, "year", 2);
      expect(result).toEqual(new Date(Date.UTC(2028, 0, 1)));
    });

    it("advances by negative count", () => {
      const date = new Date(Date.UTC(2026, 0, 1));
      const result = advanceDate(date, "year", -2);
      expect(result).toEqual(new Date(Date.UTC(2024, 0, 1)));
    });
  });
});

describe("formatDateSuffix", () => {
  it("formats day suffix", () => {
    const date = new Date(Date.UTC(2026, 0, 21));
    expect(formatDateSuffix(date, "day")).toBe("20260121");
  });

  it("formats ISO week suffix", () => {
    // Confirmed against Postgres to_char(date, 'IYYY"w"IW').
    expect(formatDateSuffix(new Date(Date.UTC(2023, 0, 9)), "week")).toBe(
      "2023w02",
    );
    expect(formatDateSuffix(new Date(Date.UTC(2026, 7, 10)), "week")).toBe(
      "2026w33",
    );
  });

  it("formats ISO week suffix using the ISO week-year at boundaries", () => {
    // 2025-12-29 belongs to ISO week 2026-W01; 2024-12-30 to 2025-W01.
    expect(formatDateSuffix(new Date(Date.UTC(2025, 11, 29)), "week")).toBe(
      "2026w01",
    );
    expect(formatDateSuffix(new Date(Date.UTC(2024, 11, 30)), "week")).toBe(
      "2025w01",
    );
    expect(formatDateSuffix(new Date(Date.UTC(2026, 11, 28)), "week")).toBe(
      "2026w53",
    );
  });

  it("formats month suffix", () => {
    const date = new Date(Date.UTC(2026, 0, 1));
    expect(formatDateSuffix(date, "month")).toBe("202601");
  });

  it("formats year suffix", () => {
    const date = new Date(Date.UTC(2026, 0, 1));
    expect(formatDateSuffix(date, "year")).toBe("2026");
  });

  it("pads single-digit month and day with zeros", () => {
    const date = new Date(Date.UTC(2026, 0, 5));
    expect(formatDateSuffix(date, "day")).toBe("20260105");
    expect(formatDateSuffix(date, "month")).toBe("202601");
  });
});

describe("DateRanges", () => {
  describe("day period", () => {
    it("generates correct ranges with past and future", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 0, 21)),
        period: "day",
        past: 1,
        future: 1,
      });

      const result = [...ranges];
      expect(result).toHaveLength(3);

      expect(result[0]).toEqual({
        start: new Date(Date.UTC(2026, 0, 20)),
        end: new Date(Date.UTC(2026, 0, 21)),
        suffix: "20260120",
      });
      expect(result[1]).toEqual({
        start: new Date(Date.UTC(2026, 0, 21)),
        end: new Date(Date.UTC(2026, 0, 22)),
        suffix: "20260121",
      });
      expect(result[2]).toEqual({
        start: new Date(Date.UTC(2026, 0, 22)),
        end: new Date(Date.UTC(2026, 0, 23)),
        suffix: "20260122",
      });
    });
  });

  describe("month period", () => {
    it("generates correct ranges with past and future", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 0, 15)),
        period: "month",
        past: 1,
        future: 2,
      });

      const result = [...ranges];
      expect(result).toHaveLength(4);

      expect(result[0]).toEqual({
        start: new Date(Date.UTC(2025, 11, 1)),
        end: new Date(Date.UTC(2026, 0, 1)),
        suffix: "202512",
      });
      expect(result[1]).toEqual({
        start: new Date(Date.UTC(2026, 0, 1)),
        end: new Date(Date.UTC(2026, 1, 1)),
        suffix: "202601",
      });
      expect(result[2]).toEqual({
        start: new Date(Date.UTC(2026, 1, 1)),
        end: new Date(Date.UTC(2026, 2, 1)),
        suffix: "202602",
      });
      expect(result[3]).toEqual({
        start: new Date(Date.UTC(2026, 2, 1)),
        end: new Date(Date.UTC(2026, 3, 1)),
        suffix: "202603",
      });
    });
  });

  describe("year period", () => {
    it("generates correct ranges with past and future", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 5, 15)),
        period: "year",
        past: 1,
        future: 1,
      });

      const result = [...ranges];
      expect(result).toHaveLength(3);

      expect(result[0]).toEqual({
        start: new Date(Date.UTC(2025, 0, 1)),
        end: new Date(Date.UTC(2026, 0, 1)),
        suffix: "2025",
      });
      expect(result[1]).toEqual({
        start: new Date(Date.UTC(2026, 0, 1)),
        end: new Date(Date.UTC(2027, 0, 1)),
        suffix: "2026",
      });
      expect(result[2]).toEqual({
        start: new Date(Date.UTC(2027, 0, 1)),
        end: new Date(Date.UTC(2028, 0, 1)),
        suffix: "2027",
      });
    });
  });

  describe("week period", () => {
    it("generates Monday-aligned ISO-week ranges with past and future", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 7, 15)),
        period: "week",
        past: 1,
        future: 1,
      });

      const result = [...ranges];
      expect(result).toHaveLength(3);

      expect(result[0]).toEqual({
        start: new Date(Date.UTC(2026, 7, 3)),
        end: new Date(Date.UTC(2026, 7, 10)),
        suffix: "2026w32",
      });
      expect(result[1]).toEqual({
        start: new Date(Date.UTC(2026, 7, 10)),
        end: new Date(Date.UTC(2026, 7, 17)),
        suffix: "2026w33",
      });
      expect(result[2]).toEqual({
        start: new Date(Date.UTC(2026, 7, 17)),
        end: new Date(Date.UTC(2026, 7, 24)),
        suffix: "2026w34",
      });
    });
  });

  describe("edge cases", () => {
    it("handles zero past and future (single partition)", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 0, 21)),
        period: "month",
        past: 0,
        future: 0,
      });

      const result = [...ranges];
      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        start: new Date(Date.UTC(2026, 0, 1)),
        end: new Date(Date.UTC(2026, 1, 1)),
        suffix: "202601",
      });
    });

    it("handles only past partitions", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 0, 21)),
        period: "month",
        past: 2,
        future: 0,
      });

      const result = [...ranges];
      expect(result.map((r) => r.suffix)).toEqual([
        "202511",
        "202512",
        "202601",
      ]);
    });

    it("handles only future partitions", () => {
      const ranges = new DateRanges({
        today: new Date(Date.UTC(2026, 0, 21)),
        period: "month",
        past: 0,
        future: 2,
      });

      const result = [...ranges];
      expect(result.map((r) => r.suffix)).toEqual([
        "202601",
        "202602",
        "202603",
      ]);
    });
  });
});

describe("parsePartitionDate", () => {
  it("parses an ISO-week suffix back to its Monday", () => {
    expect(parsePartitionDate("posts_2026w33", "week")).toEqual(
      new Date(Date.UTC(2026, 7, 10)),
    );
  });

  it("parses an ISO-week suffix across a year boundary", () => {
    // 2026-W01 starts on 2025-12-29.
    expect(parsePartitionDate("visits_2026w01", "week")).toEqual(
      new Date(Date.UTC(2025, 11, 29)),
    );
  });

  it("round-trips week suffixes through formatDateSuffix", () => {
    for (const iso of [
      "2023-01-09",
      "2026-08-10",
      "2024-12-30",
      "2026-12-28",
    ]) {
      const monday = new Date(`${iso}T00:00:00Z`);
      const suffix = formatDateSuffix(monday, "week");
      expect(parsePartitionDate(`t_${suffix}`, "week")).toEqual(monday);
    }
  });

  it("parses month and day suffixes", () => {
    expect(parsePartitionDate("posts_202601", "month")).toEqual(
      new Date(Date.UTC(2026, 0, 1)),
    );
    expect(parsePartitionDate("posts_20260121", "day")).toEqual(
      new Date(Date.UTC(2026, 0, 21)),
    );
  });
});
