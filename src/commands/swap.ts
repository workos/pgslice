import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class SwapCommand extends BaseCommand {
  static override paths = [["swap"]];

  static override usage = Command.Usage({
    description: "Swap the intermediate table with the original table",
    details: `
      This command performs the final step in the partitioning workflow by atomically
      exchanging the original table with the intermediate (partitioned) table.

      After the swap:
      - The original table becomes \`{table}_retired\`
      - The intermediate table becomes \`{table}\` (the main table)
      - Sequence ownership is transferred to the new main table
      - A retired mirroring trigger is enabled to keep the retired table in sync
    `,
    examples: [
      ["Swap tables", "$0 swap posts"],
      ["With custom lock timeout", "$0 swap posts --lock-timeout 10s"],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  lockTimeout = Option.String("--lock-timeout", "5s", {
    description: "Lock timeout for the swap operation",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.swap(tx, {
        table: this.table,
        lockTimeout: this.lockTimeout,
      });
    });
    this.context.stdout.write(
      `Swapped ${this.table} with intermediate table\n`,
    );
  }
}
