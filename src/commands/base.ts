import { Command, Option, BaseContext } from "clipanion";
import { Pgslice } from "../pgslice.js";

interface Context extends BaseContext {
  pgslice: Pgslice;
}

export class BaseCommand extends Command<Context> {
  url = Option.String("--url", {
    description: "Database connection URL (default: PGSLICE_URL env var)",
    required: false,
  });

  dryRun = Option.Boolean("--dry-run", false, {
    description: "Print statements without executing",
  });

  protected getDatabaseUrl(): string {
    const url = this.url ?? process.env.PGSLICE_URL;
    if (!url) {
      throw new Error("Set PGSLICE_URL or use the --url option");
    }
    return url;
  }

  async execute() {
    this.context.pgslice = await Pgslice.connect(
      new URL(this.getDatabaseUrl()),
      {
        dryRun: this.dryRun,
      },
    );
  }
}
