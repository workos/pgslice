import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class DisableMirroringCommand extends BaseCommand {
  static override paths = [["mirroring", "disable"], ["disable_mirroring"]];

  static override usage = Command.Usage({
    description: "Disable mirroring triggers after partitioning is complete",
    details: `
      This command turns off the trigger on the source table that mirrors
      operations to the intermediate table. Use this after partitioning is
      complete.

      The trigger is disabled, not dropped: on a partitioned table that takes a
      light lock on the parent first and never blocks reads. Pass --drop to
      remove the trigger and its function instead, which takes an ACCESS
      EXCLUSIVE lock on the table for the duration of the drop. Either way the
      lock wait is bounded by --lock-timeout.
    `,
    examples: [
      ["Disable mirroring for a table", "$0 mirroring disable posts"],
      [
        "Disable mirroring with explicit schema",
        "$0 mirroring disable myschema.posts",
      ],
      [
        "Remove the trigger and function entirely",
        "$0 mirroring disable posts --drop",
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
  drop = Option.Boolean("--drop", false, {
    description: "Drop the trigger and its function instead of disabling",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.disableMirroring(tx, {
        table: this.table,
        lockTimeout: this.lockTimeout,
        drop: this.drop,
      });
      this.context.stdout.write(
        this.drop
          ? `Mirroring triggers dropped for ${this.table}\n`
          : `Mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
