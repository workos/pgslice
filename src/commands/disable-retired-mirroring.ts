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
      This command turns off the trigger on the source table that mirrors
      operations to the retired table. Use this when you no longer need to keep
      the retired table in sync with the main table.

      The trigger is disabled, not dropped: on a partitioned table that takes a
      light lock on the parent first and never blocks reads. Pass --drop to
      remove the trigger and its function instead, which takes an ACCESS
      EXCLUSIVE lock on the table for the duration of the drop. Either way the
      lock wait is bounded by --lock-timeout.
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
        "Remove the trigger and function entirely",
        "$0 mirroring disable-retired posts --drop",
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
  drop = Option.Boolean("--drop", false, {
    description: "Drop the trigger and its function instead of disabling",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    await pgslice.start(async (tx) => {
      await pgslice.disableMirroring(tx, {
        table: this.table,
        targetType: "retired",
        lockTimeout: this.lockTimeout,
        drop: this.drop,
      });
      this.context.stdout.write(
        this.drop
          ? `Retired mirroring triggers dropped for ${this.table}\n`
          : `Retired mirroring triggers disabled for ${this.table}\n`,
      );
    });
  }
}
