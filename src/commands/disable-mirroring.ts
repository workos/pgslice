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

      The table is locked before the trigger is dropped, parent first and
      then its partitions, the same order application statements lock in,
      so the drop cannot deadlock against live traffic. Statements on the
      table queue behind the lock for the duration; the wait is bounded by
      --lock-timeout, and a timeout aborts cleanly and can be retried.
    `,
    examples: [
      ["Disable mirroring for a table", "$0 mirroring disable posts"],
      [
        "Disable mirroring with explicit schema",
        "$0 mirroring disable myschema.posts",
      ],
      [
        "With custom lock timeout",
        "$0 mirroring disable posts --lock-timeout 10s",
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
        lockTimeout: this.lockTimeout,
      });
      this.context.stdout.write(
        `Mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
