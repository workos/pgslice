import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { Pgslice } from "../pgslice.js";

export class DropPartitionsCommand extends BaseCommand {
  static override paths = [["drop_partitions"]];

  static override usage = Command.Usage({
    description: "Drop a partitioned table's expired partitions",
    details: `
      Drops the partitions that fall entirely outside the table's retention
      window. Retention is read from the table's pgslice settings comment as
      \`retention:<N>\`: the current period plus the N periods before it are
      kept. Tables without a retention setting are never pruned.

      DEFAULT and MINVALUE/MAXVALUE partitions are never dropped, and nothing is
      dropped with CASCADE. Each partition is dropped oldest-first in its own
      short transaction; if a drop cannot acquire the parent's lock within
      --lock-timeout, the command stops and the rest is retried on the next run.
    `,
    examples: [
      [
        "Preview which partitions would be dropped",
        "$0 drop_partitions posts --dry-run",
      ],
      ["Drop expired partitions", "$0 drop_partitions posts"],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  dryRun = Option.Boolean("--dry-run", false, {
    description: "List the expired partitions without dropping them",
  });
  lockTimeout = Option.String("--lock-timeout", "5s", {
    description:
      "How long each DROP waits on the parent's lock before backing off (default: 5s)",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    const dropped = await pgslice.start(async (connection) =>
      pgslice.dropPartitions(connection, {
        table: this.table,
        dryRun: this.dryRun,
        lockTimeout: this.lockTimeout,
      }),
    );

    const verb = this.dryRun ? "would drop" : "dropped";
    this.context.stdout.write(
      dropped.length > 0
        ? `${this.table}: ${verb} ${dropped.length} partition(s): ${dropped.join(", ")}\n`
        : `${this.table}: no expired partitions\n`,
    );
  }
}
