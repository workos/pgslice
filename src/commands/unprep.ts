import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class UnprepCommand extends BaseCommand {
  static override paths = [["unprep"]];

  static override usage = Command.Usage({
    description: "Undo the prep command by dropping the intermediate table",
    details: `
      This command reverses a previous prep operation by dropping the
      intermediate table and any dependent objects (like partitions).
    `,
    examples: [
      ["Undo prep for a table", "$0 unprep posts"],
      ["With explicit schema", "$0 unprep myschema.posts"],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.unprep(tx, { table: this.table });
    });
    this.context.stdout.write(`Dropped intermediate table for ${this.table}\n`);
  }
}
