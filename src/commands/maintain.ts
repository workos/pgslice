import { Command, Option } from "clipanion";
import * as t from "typanion";

import { BaseCommand } from "./base.js";
import { Pgslice } from "../pgslice.js";
import type { MaintainLog } from "../types.js";

export class MaintainCommand extends BaseCommand {
  static override paths = [["maintain"]];

  static override usage = Command.Usage({
    description:
      "Extend every managed partitioned table discovered from the catalog",
    details: `
      Discovers every partitioned table that carries a valid pgslice settings
      comment and extends each one with add_partitions, so newly partitioned
      tables are picked up automatically with no per-table configuration.

      Native tables (the partitioned parent owns the primary key) and classic
      pgslice tables (each partition owns its own) are both handled. After
      extending each table this checks that every leaf partition has a replica
      identity usable for logical replication, and exits non-zero if any does
      not.
    `,
    examples: [
      [
        "Extend all managed tables with the default per-period horizons",
        "$0 maintain",
      ],
      [
        "Override one period's horizon (12 months of monthly runway)",
        "$0 maintain --future-monthly 12",
      ],
      [
        "Configure a horizon via environment (e.g. from a scheduled job)",
        "PGSLICE_FUTURE_MONTHLY=12 $0 maintain",
      ],
      ["Restrict to a single schema", "$0 maintain --schema analytics"],
    ],
  });

  past = Option.String("--past", "0", {
    description: "Number of past partitions to add to each table",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  futureDaily = Option.String("--future-daily", "90", {
    description: "Future partitions to keep ahead for daily tables",
    env: "PGSLICE_FUTURE_DAILY",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  futureWeekly = Option.String("--future-weekly", "26", {
    description: "Future partitions to keep ahead for weekly tables",
    env: "PGSLICE_FUTURE_WEEKLY",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  futureMonthly = Option.String("--future-monthly", "6", {
    description: "Future partitions to keep ahead for monthly tables",
    env: "PGSLICE_FUTURE_MONTHLY",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  futureYearly = Option.String("--future-yearly", "1", {
    description: "Future partitions to keep ahead for yearly tables",
    env: "PGSLICE_FUTURE_YEARLY",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  schema = Option.String("--schema", {
    description: "Restrict maintenance to partitioned tables in this schema",
  });
  tablespace = Option.String("--tablespace", {
    description: "Tablespace to use for new partitions",
  });
  inheritGrants = Option.Boolean("--inherit-grants", true, {
    description:
      "Copy each parent table's privileges onto its new partitions (default: true; disable with --no-inherit-grants)",
  });

  override async perform(pgslice: Pgslice): Promise<number | void> {
    // maintain emits structured records (start, per-table, final); write each as
    // one JSON object per line (JSONL).
    const log: MaintainLog = (entry) => {
      this.context.stdout.write(`${JSON.stringify(entry)}\n`);
    };

    // Host only — never the credentials in the URL — so the logs identify the
    // endpoint being maintained without leaking secrets.
    const host = new URL(this.getDatabaseUrl()).hostname;

    const results = await pgslice.start((connection) =>
      pgslice.maintain(
        connection,
        {
          host,
          past: this.past,
          futureDaily: this.futureDaily,
          futureWeekly: this.futureWeekly,
          futureMonthly: this.futureMonthly,
          futureYearly: this.futureYearly,
          schema: this.schema,
          tablespace: this.tablespace,
          inheritGrants: this.inheritGrants,
        },
        log,
      ),
    );

    // Exit non-zero if any table failed to extend or shipped a CDC-unsafe leaf;
    // as a CronJob that surfaces as a failed Job. The final summary log already
    // carries the detail, so no separate error line is written.
    const failed = results.some(
      (result) => result.error !== null || !result.replicaIdentityReady,
    );
    return failed ? 1 : undefined;
  }
}
