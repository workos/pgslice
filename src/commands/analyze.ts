import { Command, Option, type Usage } from "clipanion";

import { Pgslice } from "../pgslice.js";
import { BaseCommand } from "./base.js";

/**
 * Analyze command for updating PostgreSQL statistics on a table.
 */
export class AnalyzeCommand extends BaseCommand {
  static override paths = [["analyze"]];

  static override usage: Usage = Command.Usage({
    description: "Analyze a table to update statistics",
    details: `
      Runs ANALYZE VERBOSE on the specified table to update PostgreSQL statistics.

      By default, analyzes the intermediate table (before swap).
      With --swapped, analyzes the main table (after swap).
    `,
    examples: [
      ["Analyze intermediate table", "$0 analyze posts"],
      ["Analyze main table after swap", "$0 analyze posts --swapped"],
    ],
  });

  table = Option.String({ required: true, name: "table" });

  swapped = Option.Boolean("--swapped", false, {
    description: "Analyze the main table instead of the intermediate table",
  });

  override async perform(pgslice: Pgslice): Promise<void> {
    const table = await pgslice.analyze({
      table: this.table,
      swapped: this.swapped,
    });

    this.context.stdout.write(`ANALYZE VERBOSE ${table.quoted};\n`);
  }
}
