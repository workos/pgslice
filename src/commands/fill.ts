import { Command, Option, type Usage } from "clipanion";
import { Pgslice } from "../pgslice.js";
import { BaseCommand } from "./base.js";

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
  });

  swapped = Option.Boolean("--swapped", false, {
    description: "Use swapped table (source=retired, dest=original)",
  });

  sourceTable = Option.String("--source-table", {
    description: "Override source table",
  });

  destTable = Option.String("--dest-table", {
    description: "Override destination table",
  });

  start = Option.String("--start", {
    description: "Primary key value to start from (numeric or ULID)",
  });

  sleep = Option.String("--sleep", {
    description: "Seconds to sleep between batches",
  });

  protected getDatabaseUrl(): string {
    const url = this.url ?? process.env.PGSLICE_URL;
    if (!url) {
      throw new Error("Set PGSLICE_URL or use the --url option");
    }
    return url;
  }

  async perform(pgslice: Pgslice) {
    const batchSize = parseInt(this.batchSize, 10);
    if (isNaN(batchSize) || batchSize <= 0) {
      throw new Error("Invalid batch size");
    }

    const sleepSeconds = this.sleep ? parseFloat(this.sleep) : undefined;
    if (
      sleepSeconds !== undefined &&
      (isNaN(sleepSeconds) || sleepSeconds < 0)
    ) {
      throw new Error("Invalid sleep value");
    }

    let hasBatches = false;
    for await (const batch of pgslice.fill({
      table: this.table,
      swapped: this.swapped,
      sourceTable: this.sourceTable,
      destTable: this.destTable,
      batchSize,
      start: this.start,
    })) {
      hasBatches = true;

      // Format progress message
      const batchLabel =
        batch.totalBatches !== null
          ? `${batch.batchNumber} of ${batch.totalBatches}`
          : `batch ${batch.batchNumber}`;

      this.context.stdout.write(`/* ${batchLabel} */\n`);

      // Sleep between batches if requested
      if (sleepSeconds !== undefined && sleepSeconds > 0) {
        await this.#sleep(sleepSeconds * 1000);
      }
    }

    if (!hasBatches) {
      this.context.stdout.write("/* nothing to fill */\n");
    }
  }

  #sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
