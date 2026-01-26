import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class DisableRetiredMirroringCommand extends BaseCommand {
  static override paths = [
    ["mirroring", "disable-retired"],
    ["disable_retired_mirroring"],
  ];

  static override usage = Command.Usage({
    description: "Disable retired mirroring triggers",
    details: `
      This command removes the triggers on the source table that mirror operations
      to the retired table. Use this when you no longer need to keep the retired
      table in sync with the main table.
    `,
    examples: [
      [
        "Disable retired mirroring for a table",
        "$0 mirroring disable-retired posts",
      ],
      [
        "Disable retired mirroring with explicit schema",
        "$0 mirroring disable-retired myschema.posts",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.disableMirroring(tx, {
        table: this.table,
        targetType: "retired",
      });
      this.context.stdout.write(
        `Retired mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
