import { Command, Option } from "clipanion";
import { DatabaseTransactionConnection } from "slonik";

import { BaseCommand } from "./base.js";

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
      ["Add partitions to intermediate table", "$0 add_partitions posts --intermediate --past 1 --future 3"],
      ["Add partitions after swap", "$0 add_partitions posts --future 3"],
      ["Specify tablespace", "$0 add_partitions posts --intermediate --future 3 --tablespace fast_storage"],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  intermediate = Option.Boolean("--intermediate", false, {
    description: "Add to intermediate table",
  });
  past = Option.String("--past", "0", {
    description: "Number of past partitions to add",
  });
  future = Option.String("--future", "0", {
    description: "Number of future partitions to add",
  });
  tablespace = Option.String("--tablespace", {
    description: "Tablespace to use for new partitions",
  });

  override async perform(tx: DatabaseTransactionConnection): Promise<void> {
    const past = parseInt(this.past, 10);
    const future = parseInt(this.future, 10);

    if (isNaN(past) || past < 0) {
      throw new Error("--past must be a non-negative integer");
    }

    if (isNaN(future) || future < 0) {
      throw new Error("--future must be a non-negative integer");
    }

    await this.context.pgslice.addPartitions(tx, {
      table: this.table,
      intermediate: this.intermediate,
      past,
      future,
      tablespace: this.tablespace,
    });
  }
}
