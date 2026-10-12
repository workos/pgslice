import { Command, Option } from "clipanion";

import { BaseCommand } from "./base.js";
import { type Pgslice } from "../pgslice.js";

export class StatusCommand extends BaseCommand {
  static override paths = [["status"]];

  static override usage = Command.Usage({
    description: "Show status information about a table's partitioning state",
    details: `
      This command displays the current state of a table's partitioning workflow,
      including:
      - Whether intermediate table exists
      - Number of partitions
      - Whether mirroring triggers are enabled
      - Whether the original table is partitioned
    `,
    examples: [
      ["Show status for a table", "$0 status posts"],
      ["Show status with explicit schema", "$0 status myschema.posts"],
      ["Output as JSON", "$0 status posts --json"],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  json = Option.Boolean("--json", false, {
    description: "Output status as JSON",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    const status = await pgslice.status({ table: this.table });

    if (this.json) {
      this.context.stdout.write(JSON.stringify(status, null, 2) + "\n");
    } else {
      this.context.stdout.write(`Table: ${this.table}\n`);
      this.context.stdout.write(
        `Intermediate exists: ${status.intermediateExists}\n`,
      );
      this.context.stdout.write(
        `Original is partitioned: ${status.originalIsPartitioned}\n`,
      );
      this.context.stdout.write(`Partition count: ${status.partitionCount}\n`);
      this.context.stdout.write(
        `Mirror trigger exists: ${status.mirrorTriggerExists}\n`,
      );
      this.context.stdout.write(
        `Retired mirror trigger exists: ${status.retiredMirrorTriggerExists}\n`,
      );
    }
  }
}
