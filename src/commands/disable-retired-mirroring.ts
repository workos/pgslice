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

      The table is locked before the trigger is dropped, parent first and
      then its partitions, the same order application statements lock in,
      so the drop cannot deadlock against live traffic. Statements on the
      table queue behind the lock for the duration; the wait is bounded by
      --lock-timeout, and a timeout aborts cleanly and can be retried.
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
      [
        "With custom lock timeout",
        "$0 mirroring disable-retired posts --lock-timeout 10s",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  lockTimeout = Option.String("--lock-timeout", "5s", {
    description: "Bound on how long to wait for the table lock",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.disableMirroring(tx, {
        table: this.table,
        targetType: "retired",
        lockTimeout: this.lockTimeout,
      });
      this.context.stdout.write(
        `Retired mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
