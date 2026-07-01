import { Command, Option } from "clipanion";
import * as t from "typanion";

import { BaseCommand } from "./base.js";
import { Pgslice } from "../pgslice.js";

export class AddPartitionsCommand extends BaseCommand {
  static override paths = [["add_partitions"]];

  static override usage = Command.Usage({
    description: "Add partitions to a partitioned table",
    details: `
      This command adds partitions to a partitioned table based on its settings.

      By default, it targets the original table (after swap). Use --intermediate
      to add partitions to the intermediate table before swapping.

      The --past and --future options control how many partitions to create
      relative to the current date.
    `,
    examples: [
      [
        "Add partitions to intermediate table",
        "$0 add_partitions posts --intermediate --past 1 --future 3",
      ],
      ["Add partitions after swap", "$0 add_partitions posts --future 3"],
      [
        "Specify tablespace",
        "$0 add_partitions posts --intermediate --future 3 --tablespace fast_storage",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  intermediate = Option.Boolean("--intermediate", false, {
    description: "Add to intermediate table",
  });
  past = Option.String("--past", "0", {
    description: "Number of past partitions to add",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  future = Option.String("--future", "0", {
    description: "Number of future partitions to add",
    validator: t.cascade(t.isNumber(), [t.isInteger(), t.isAtLeast(0)]),
  });
  tablespace = Option.String("--tablespace", {
    description: "Tablespace to use for new partitions",
  });
  inheritGrants = Option.Boolean("--inherit-grants", true, {
    description:
      "Copy the parent table's privileges onto each new partition (default: true; disable with --no-inherit-grants)",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    const created = await pgslice.start(async (tx) =>
      pgslice.addPartitions(tx, {
        table: this.table,
        intermediate: this.intermediate,
        past: this.past,
        future: this.future,
        tablespace: this.tablespace,
        inheritGrants: this.inheritGrants,
      }),
    );

    this.context.stdout.write(
      created.length > 0
        ? `${this.table}: +${created.length} partition(s): ${created.join(", ")}\n`
        : `${this.table}: no new partitions needed\n`,
    );
  }
}
