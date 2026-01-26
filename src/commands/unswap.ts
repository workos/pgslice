import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class UnswapCommand extends BaseCommand {
  static override paths = [["unswap"]];

  static override usage = Command.Usage({
    description: "Unswap the retired table back to the original table",
    details: `
      This command reverses a previous swap operation, restoring the original
      (non-partitioned) table as the main table.

      After the unswap:
      - The original table becomes \`{table}_intermediate\`
      - The retired table becomes \`{table}\` (the main table)
      - Sequence ownership is transferred to the new main table
      - An intermediate mirroring trigger is enabled to keep the intermediate table in sync
    `,
    examples: [
      ["Unswap tables", "$0 unswap posts"],
      ["With custom lock timeout", "$0 unswap posts --lock-timeout 10s"],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  lockTimeout = Option.String("--lock-timeout", "5s", {
    description: "Lock timeout for the unswap operation",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.unswap(tx, {
        table: this.table,
        lockTimeout: this.lockTimeout,
      });
    });
    this.context.stdout.write(`Unswapped ${this.table} with retired table\n`);
  }
}
