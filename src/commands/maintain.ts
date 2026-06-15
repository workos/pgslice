import { Command, Option } from "clipanion";
import * as t from "typanion";

import { BaseCommand } from "./base.js";
import { Pgslice } from "../pgslice.js";

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
        "Extend all managed tables three periods ahead",
        "$0 maintain --future 3",
      ],
      [
        "Restrict to a single schema",
        "$0 maintain --future 3 --schema analytics",
      ],
    ],
  });

  past = Option.String("--past", "0", {
    description: "Number of past partitions to add to each table",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  future = Option.String("--future", "0", {
    description: "Number of future partitions to add to each table",
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

  override async perform(pgslice: Pgslice): Promise<void> {
    const results = await pgslice.start((connection) =>
      pgslice.maintain(connection, {
        past: this.past,
        future: this.future,
        schema: this.schema,
        tablespace: this.tablespace,
        inheritGrants: this.inheritGrants,
      }),
    );

    if (results.length === 0) {
      this.context.stdout.write("No managed partitioned tables found.\n");
      return;
    }

    const problems: string[] = [];
    for (const result of results) {
      if (result.error) {
        this.context.stdout.write(
          `${result.table}: FAILED — ${result.error}\n`,
        );
        problems.push(`${result.table}: ${result.error}`);
        continue;
      }

      this.context.stdout.write(
        `${result.table} [${result.model}]: +${result.partitionsCreated.length} partition(s), ${result.partitionCount} total\n`,
      );
      if (!result.replicaIdentityReady) {
        problems.push(
          `${result.table}: partitions without a usable replica identity (CDC-unsafe): ${result.unsafePartitions.join(
            ", ",
          )}`,
        );
      }
    }

    if (problems.length > 0) {
      throw new Error(
        `maintain encountered problems:\n  ${problems.join("\n  ")}`,
      );
    }
  }
}
