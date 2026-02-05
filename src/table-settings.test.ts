import { describe, expect, it } from "vitest";
import { TableSettings } from "./table-settings.js";

describe("TableSettings", () => {
  describe("constructor", () => {
    it("creates an instance with the given properties", () => {
      const settings = new TableSettings("created_at", "month", "date");

      expect(settings.column).toBe("created_at");
      expect(settings.period).toBe("month");
      expect(settings.cast).toBe("date");
    });
  });

  describe("parseFromComment", () => {
    it("parses a valid comment string", () => {
      const settings = TableSettings.parseFromComment(
        "column:created_at,period:month,cast:date",
      );

      expect(settings).not.toBeNull();
      expect(settings?.column).toBe("created_at");
      expect(settings?.period).toBe("month");
      expect(settings?.cast).toBe("date");
    });

    it("parses with different order of fields", () => {
      const settings = TableSettings.parseFromComment(
        "period:year,cast:timestamptz,column:updated_at",
      );

      expect(settings).not.toBeNull();
      expect(settings?.column).toBe("updated_at");
      expect(settings?.period).toBe("year");
      expect(settings?.cast).toBe("timestamptz");
    });

    describe("periods", () => {
      it("parses day period", () => {
        const settings = TableSettings.parseFromComment(
          "column:ts,period:day,cast:date",
        );
        expect(settings?.period).toBe("day");
      });

      it("parses month period", () => {
        const settings = TableSettings.parseFromComment(
          "column:ts,period:month,cast:date",
        );
        expect(settings?.period).toBe("month");
      });

      it("parses year period", () => {
        const settings = TableSettings.parseFromComment(
          "column:ts,period:year,cast:date",
        );
        expect(settings?.period).toBe("year");
      });
    });

    describe("casts", () => {
      it("parses date cast", () => {
        const settings = TableSettings.parseFromComment(
          "column:ts,period:month,cast:date",
        );
        expect(settings?.cast).toBe("date");
      });

      it("parses timestamptz cast", () => {
        const settings = TableSettings.parseFromComment(
          "column:ts,period:month,cast:timestamptz",
        );
        expect(settings?.cast).toBe("timestamptz");
      });
    });

    describe("invalid comments", () => {
      it("returns null for empty string", () => {
        expect(TableSettings.parseFromComment("")).toBeNull();
      });

      it("returns null when column is missing", () => {
        expect(
          TableSettings.parseFromComment("period:month,cast:date"),
        ).toBeNull();
      });

      it("returns null when period is missing", () => {
        expect(
          TableSettings.parseFromComment("column:created_at,cast:date"),
        ).toBeNull();
      });

      it("returns null when cast is missing", () => {
        expect(
          TableSettings.parseFromComment("column:created_at,period:month"),
        ).toBeNull();
      });

      it("returns null for invalid period", () => {
        expect(
          TableSettings.parseFromComment(
            "column:created_at,period:weekly,cast:date",
          ),
        ).toBeNull();
      });

      it("returns null for invalid cast", () => {
        expect(
          TableSettings.parseFromComment(
            "column:created_at,period:month,cast:timestamp",
          ),
        ).toBeNull();
      });

      it("returns null for unrelated comment", () => {
        expect(
          TableSettings.parseFromComment("This is a regular table comment"),
        ).toBeNull();
      });
    });
  });
});
