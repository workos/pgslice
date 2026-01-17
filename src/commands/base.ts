import { Command, Option, BaseContext } from "clipanion";
import { Pgslice } from "../pgslice.js";

interface Context extends BaseContext {
  pgslice: Pgslice;
}

export class BaseCommand extends Command<Context> {
  url = Option.String("DATABASE_URL", {
    description: "Database connection URL",
    required: true,
  });

  dryRun = Option.Boolean("--dry-run", false, {
    description: "Run the command in dry-run mode",
  });

  async execute(): Promise<number> {
    this.context.pgslice = await Pgslice.connect(new URL(this.url), {
      dryRun: this.dryRun,
    });

    return 0;
  }
}
