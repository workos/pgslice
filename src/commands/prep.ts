import { Command, Option } from "clipanion";
import { DatabaseTransactionConnection } from "slonik";

import type { Period } from "../types.js";
import { BaseCommand } from "./base.js";

export class PrepCommand extends BaseCommand {
  static override paths = [["prep"]];

  static override usage = Command.Usage({
    description: "Create an intermediate table for partitioning",
    details: `
      This command creates an intermediate table that will be used for partitioning.
      By default, it creates a partitioned table using PARTITION BY RANGE.

      If --no-partition is specified, it creates a regular table without partitioning.
      This is useful when you want to use pgslice for data migration without partitioning.
    `,
    examples: [
      [
        "Create a partitioned intermediate table",
        "$0 prep posts created_at month",
      ],
      [
        "Create with explicit schema",
        "$0 prep myschema.posts created_at month",
      ],
      [
        "Create a non-partitioned intermediate table",
        "$0 prep posts --no-partition",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });
  column = Option.String({ required: false, name: "column" });
  period = Option.String({ required: false, name: "period" });
  partition = Option.Boolean("--partition", true, {
    description: "Create a partitioned table (default: true)",
  });

  override async perform(tx: DatabaseTransactionConnection): Promise<void> {
    try {
      await this.context.pgslice.prep(tx, {
        table: this.table,
        column: this.column,
        period: this.period as Period | undefined,
        partition: this.partition,
      });
    } catch (error) {
      console.error(error);
      throw error;
    }
  }
}
