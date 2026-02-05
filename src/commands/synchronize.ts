import { Command, Option, type Usage } from "clipanion";
import * as t from "typanion";

import { Pgslice } from "../pgslice.js";
import { BaseCommand } from "./base.js";
import type { SynchronizeBatchResult } from "../types.js";
import { sleep } from "../command-utils.js";

/**
 * Synchronize command for detecting and fixing data discrepancies between tables.
 */
export class SynchronizeCommand extends BaseCommand {
  static override paths = [["synchronize"]];

  static override usage: Usage = Command.Usage({
    description: "Synchronize data between two tables",
    details: `
      This command compares rows between a source table (TABLE) and its
      intermediate table (TABLE_intermediate), detecting and fixing discrepancies
      via INSERT, UPDATE, and DELETE operations.

      The source table is the authoritative data source. The target table
      (intermediate) is updated to match.

      Use --dry-run to see what changes would be made without executing them.
    `,
    examples: [
      ["Synchronize posts to intermediate", "$0 synchronize posts"],
      [
        "Synchronize with custom window size",
        "$0 synchronize posts --window-size 500",
      ],
      ["Dry run to see changes", "$0 synchronize posts --dry-run"],
      ["Start from a specific ID", "$0 synchronize posts --start 12345"],
      [
        "With delay between batches",
        "$0 synchronize posts --delay 1 --delay-multiplier 0.5",
      ],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  start = Option.String("--start", {
    description: "Primary key value to start synchronization at",
  });

  windowSize = Option.String("--window-size", "1000", {
    description: "Number of rows to synchronize per batch",
    validator: t.cascade(t.isNumber(), [t.isAtLeast(1)]),
  });

  delay = Option.String("--delay", "0", {
    description: "Base delay in seconds between batches",
    validator: t.cascade(t.isNumber(), [t.isAtLeast(0)]),
  });

  delayMultiplier = Option.String("--delay-multiplier", "0", {
    description: "Delay multiplier for batch time",
    validator: t.cascade(t.isNumber(), [t.isAtLeast(0)]),
  });

  async perform(pgslice: Pgslice) {
    // Track statistics
    const stats = {
      totalBatches: 0,
      totalRowsCompared: 0,
      matchingRows: 0,
      rowsWithDifferences: 0,
      missingRows: 0,
      extraRows: 0,
    };

    let sourceName: string | null = null;
    let targetName: string | null = null;
    let headerPrinted = false;

    for await (const batch of pgslice.synchronize({
      table: this.table,
      start: this.start,
      windowSize: this.windowSize,
      dryRun: this.dryRun,
    })) {
      // Print header on first batch (we need synchronizer to know table names)
      if (!headerPrinted) {
        // Get table names from the batch (inferred from the command options)
        sourceName = this.table;
        targetName = `${this.table}_intermediate`;
        this.#printHeader(sourceName, targetName);
        headerPrinted = true;
      }

      stats.totalBatches++;
      stats.totalRowsCompared += batch.rowsCompared;
      stats.matchingRows += batch.matchingRows;
      stats.rowsWithDifferences += batch.rowsUpdated;
      stats.missingRows += batch.rowsInserted;
      stats.extraRows += batch.rowsDeleted;

      this.#printBatchResult(batch);

      // Calculate and apply adaptive delay
      const sleepTime = this.#calculateSleepTime(batch.batchDurationMs);
      if (sleepTime > 0) {
        await sleep(sleepTime * 1000);
      }
    }

    // Print summary
    this.#printSummary(stats);
  }

  #printHeader(sourceName: string, targetName: string): void {
    const mode = this.dryRun
      ? "DRY RUN (logging only)"
      : "WRITE (executing changes)";

    this.context.stderr.write(`Synchronizing ${sourceName} to ${targetName}\n`);
    this.context.stderr.write(`Mode: ${mode}\n`);
    this.context.stderr.write("\n");
  }

  #printBatchResult(batch: SynchronizeBatchResult): void {
    const timestamp = new Date().toISOString().replace("T", " ").slice(0, 19);
    const { start, end } = batch.primaryKeyRange;
    const pkRange = start === end ? String(start) : `${start}...${end}`;

    const differencesCount =
      batch.rowsInserted + batch.rowsUpdated + batch.rowsDeleted;

    if (differencesCount > 0) {
      this.context.stderr.write(
        `[${timestamp}] Batch ${batch.batchNumber}: Found ${differencesCount} differences (keys in range ${pkRange})\n`,
      );
    } else {
      this.context.stderr.write(
        `[${timestamp}] Batch ${batch.batchNumber}: All ${batch.rowsCompared} rows match (keys in range ${pkRange})\n`,
      );
    }
  }

  #printSummary(stats: {
    totalBatches: number;
    totalRowsCompared: number;
    matchingRows: number;
    rowsWithDifferences: number;
    missingRows: number;
    extraRows: number;
  }): void {
    this.context.stderr.write("\n");
    this.context.stderr.write("Synchronization complete\n");
    this.context.stderr.write("=".repeat(50) + "\n");
    this.context.stderr.write(`Total batches: ${stats.totalBatches}\n`);
    this.context.stderr.write(
      `Total rows compared: ${stats.totalRowsCompared}\n`,
    );
    this.context.stderr.write(`Matching rows: ${stats.matchingRows}\n`);
    this.context.stderr.write(
      `Rows with differences: ${stats.rowsWithDifferences}\n`,
    );
    this.context.stderr.write(`Missing rows: ${stats.missingRows}\n`);
    this.context.stderr.write(`Extra rows: ${stats.extraRows}\n`);
  }

  #calculateSleepTime(batchDurationMs: number): number {
    // sleep = delay + (batchDurationMs/1000 * delayMultiplier)
    return this.delay + (batchDurationMs / 1000) * this.delayMultiplier;
  }
}
