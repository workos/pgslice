import { Command, Option, BaseContext } from "clipanion";
import { Pgslice } from "../pgslice.js";
import { DatabaseTransactionConnection } from "slonik";

export interface Context extends BaseContext {
  pgslice: Pgslice;
}

export abstract class BaseCommand extends Command<Context> {
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

  async execute(): Promise<number | void> {
    try {
      this.context.pgslice ??= await Pgslice.connect(
        new URL(this.getDatabaseUrl()),
        {
          dryRun: this.dryRun,
        },
      );

      await this.context.pgslice.start((transaction) =>
        this.perform(transaction),
      );
    } catch (error) {
      if (error instanceof Error) {
        this.context.stderr.write(`${error.message}\n`);
      } else {
        this.context.stderr.write(`${error}\n`);
      }
      return 1;
    } finally {
      await this.context.pgslice?.close();
    }
  }

  /**
   * The main logic of the command goes here. Avoid overriding `execute`.
   */
  protected abstract perform(
    transaction: DatabaseTransactionConnection,
  ): Promise<number | void>;
}
