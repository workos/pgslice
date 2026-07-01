import { afterEach, beforeEach, describe, expect, vi } from "vitest";

import { pgsliceTest as test } from "./testing/index.js";
import {
  addChild,
  childNames,
  nativeParent,
  setSettings,
  TS,
} from "./testing/shapes.js";

/**
 * The optional per-table `format` setting threads through `add_partitions` so a
 * retrofitted table keeps its existing naming convention. The week math is
 * unchanged (ISO) — only the rendered name differs — so these use generic names
 * that stand in for the real conventions a long-lived schema accumulates.
 */
describe("custom partition naming format", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(Date.UTC(2026, 0, 15))); // Thursday, ISO 2026-W03
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  test("applies a p{YYYY}w{WW} convention to a weekly table", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(
      transaction,
      "events_fmt",
      "created_at",
      TS,
      "week",
      "date",
    );
    await setSettings(
      transaction,
      "events_fmt",
      "created_at",
      "week",
      "date",
      "p{YYYY}w{WW}",
    );
    // Monday-aligned legacy partition (ISO 2026-W02) to anchor the extension.
    await addChild(
      transaction,
      "events_fmt",
      "events_fmt_p2026w02",
      "2026-01-05",
      "2026-01-12",
    );

    const created = await pgslice.addPartitions(transaction, {
      table: "events_fmt",
      future: 2,
    });

    expect(created).toEqual([
      "events_fmt_p2026w03",
      "events_fmt_p2026w04",
      "events_fmt_p2026w05",
    ]);
  });

  test("applies a y{YYYY}m{MM} convention to a monthly table", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(
      transaction,
      "metrics_fmt",
      "created_at",
      TS,
      "month",
      "date",
    );
    await setSettings(
      transaction,
      "metrics_fmt",
      "created_at",
      "month",
      "date",
      "y{YYYY}m{MM}",
    );
    await addChild(
      transaction,
      "metrics_fmt",
      "metrics_fmt_y2025m12",
      "2025-12-01",
      "2026-01-01",
    );

    const created = await pgslice.addPartitions(transaction, {
      table: "metrics_fmt",
      future: 1,
    });

    expect(created).toEqual(["metrics_fmt_y2026m01", "metrics_fmt_y2026m02"]);
  });

  test("is idempotent under a custom format", async ({
    pgslice,
    transaction,
  }) => {
    await nativeParent(
      transaction,
      "events_fmt",
      "created_at",
      TS,
      "week",
      "date",
    );
    await setSettings(
      transaction,
      "events_fmt",
      "created_at",
      "week",
      "date",
      "p{YYYY}w{WW}",
    );
    await addChild(
      transaction,
      "events_fmt",
      "events_fmt_p2026w02",
      "2026-01-05",
      "2026-01-12",
    );

    await pgslice.addPartitions(transaction, {
      table: "events_fmt",
      future: 2,
    });
    const second = await pgslice.addPartitions(transaction, {
      table: "events_fmt",
      future: 2,
    });

    expect(second).toEqual([]);
    expect(await childNames(transaction, "events_fmt")).toEqual([
      "events_fmt_p2026w02",
      "events_fmt_p2026w03",
      "events_fmt_p2026w04",
      "events_fmt_p2026w05",
    ]);
  });
});
