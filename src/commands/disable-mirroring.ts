import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class DisableMirroringCommand extends BaseCommand {
  static override paths = [["mirroring", "disable"], ["disable_mirroring"]];

  static override usage = Command.Usage({
    description: "Disable mirroring triggers after partitioning is complete",
    details: `
      This command removes the triggers on the source table that mirror operations
      to the intermediate table. Use this after partitioning is complete.
    `,
    examples: [
      ["Disable mirroring for a table", "$0 mirroring disable posts"],
      [
        "Disable mirroring with explicit schema",
        "$0 mirroring disable myschema.posts",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.disableMirroring(tx, { table: this.table });
      this.context.stdout.write(
        `Mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
