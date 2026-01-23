import { Command, Option } from "clipanion";
import { DatabaseTransactionConnection } from "slonik";

import { BaseCommand } from "./base.js";

export class EnableMirroringCommand extends BaseCommand {
  static override paths = [["enable_mirroring"]];

  static override usage = Command.Usage({
    description:
      "Enable mirroring triggers for live data changes during partitioning",
    details: `
      This command creates triggers on the source table that mirror INSERT, UPDATE,
      and DELETE operations to the intermediate table. This is useful during the
      partitioning process to keep the intermediate table in sync with live changes.
    `,
    examples: [
      [
        "Enable mirroring for a table",
        "$0 enable_mirroring posts",
      ],
      [
        "Enable mirroring with explicit schema",
        "$0 enable_mirroring myschema.posts",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  override async perform(tx: DatabaseTransactionConnection): Promise<void> {
    await this.context.pgslice.enableMirroring(tx, { table: this.table });
    this.context.stdout.write(`Mirroring triggers enabled for ${this.table}\n`);
  }
}
