import { describe, expect, it } from "vitest";
import {
  DateRanges,
  advanceDate,
  formatDateSuffix,
  roundDate,
} from "./date-ranges.js";

describe("roundDate", () => {
  it("rounds to start of day", () => {
    const date = new Date(Date.UTC(2026, 0, 21, 15, 30, 45));
    const result = roundDate(date, "day");
    expect(result).toEqual(new Date(Date.UTC(2026, 0, 21)));
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
