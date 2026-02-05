import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class EnableRetiredMirroringCommand extends BaseCommand {
  static override paths = [
    ["mirroring", "enable-retired"],
    ["enable_retired_mirroring"],
  ];

  static override usage = Command.Usage({
    description:
      "Enable retired mirroring triggers for syncing data to the retired table",
    details: `
      This command creates triggers on the source table that mirror INSERT, UPDATE,
      and DELETE operations to the retired table. This is typically used after a swap
      operation to keep the retired table in sync with live changes.

      The triggers include ON CONFLICT handling for inserts:
      - With primary key: DO UPDATE SET for upsert behavior
      - Without primary key: DO NOTHING to skip duplicates
    `,
    examples: [
      [
        "Enable retired mirroring for a table",
        "$0 mirroring enable-retired posts",
      ],
      [
        "Enable retired mirroring with explicit schema",
        "$0 mirroring enable-retired myschema.posts",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) =>
      pgslice.enableMirroring(tx, { table: this.table, targetType: "retired" }),
    );

    this.context.stdout.write(
      `Retired mirroring triggers enabled for ${this.table}\n`,
    );
  }
}
