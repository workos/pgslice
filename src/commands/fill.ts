import { Command, Option, type Usage } from "clipanion";
import * as t from "typanion";

import { Pgslice } from "../pgslice.js";
import { BaseCommand } from "./base.js";
import { sleep } from "../command-utils.js";

/**
 * Fill command for copying data from a source table to a destination table in batches.
 */
export class FillCommand extends BaseCommand {
  static override paths = [["fill"]];

  static override usage: Usage = Command.Usage({
    description: "Fill the partitions in batches",
    details: `
      This command copies data from a source table to a destination table in batches.
      Each batch is committed independently, allowing the operation to be resumed
      if interrupted using the --start option.

      By default:
      - Source is the original table
      - Destination is the intermediate table

      With --swapped:
      - Source is the retired table
      - Destination is the original table
    `,
    examples: [
      ["Fill data into partitions", "$0 fill posts"],
      ["Fill with custom batch size", "$0 fill posts --batch-size 5000"],
      ["Fill after swapping", "$0 fill posts --swapped"],
      ["Resume from a specific ID", "$0 fill posts --start 12345"],
      ["Fill with sleep between batches", "$0 fill posts --sleep 0.5"],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  batchSize = Option.String("--batch-size", "10000", {
    description: "Number of rows to process per batch",
    validator: t.cascade(t.isNumber(), [t.isAtLeast(1)]),
  });

  swapped = Option.Boolean("--swapped", false, {
    description: "Use swapped table (source=retired, dest=original)",
  });

  start = Option.String("--start", {
    description: "Primary key value to start from (numeric or ULID)",
  });

  sleep = Option.String("--sleep", {
    description: "Seconds to sleep between batches",
    validator: t.cascade(t.isNumber(), [t.isPositive()]),
  });

  async perform(pgslice: Pgslice) {
    await pgslice.start(async (conn) => {
      let hasBatches = false;
      for await (const batch of pgslice.fill(conn, {
        table: this.table,
        swapped: this.swapped,
        batchSize: this.batchSize,
        start: this.start,
      })) {
        hasBatches = true;

        this.context.stdout.write(`/* batch ${batch.batchNumber} */\n`);

        // Sleep between batches if requested
        if (this.sleep) {
          await sleep(this.sleep * 1000);
        }
      }

      if (!hasBatches) {
        this.context.stdout.write("/* nothing to fill */\n");
      }
    });
  }
}
