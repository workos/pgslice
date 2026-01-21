import { Command } from "clipanion";
import { DatabaseTransactionConnection, sql } from "slonik";
import { z } from "zod";
import { BaseCommand } from "./base.js";

export class HelloCommand extends BaseCommand {
  static override paths = [["hello"]];

  static override usage = Command.Usage({
    description: "A placeholder command to verify the CLI works",
  });

  override async perform(transaction: DatabaseTransactionConnection) {
    await transaction.query(
      sql.type(z.object({}))`
        CREATE TEMP TABLE pgslice_hello_test (id SERIAL PRIMARY KEY, message TEXT)
      `,
    );
    await transaction.query(
      sql.type(z.object({}))`
        INSERT INTO pgslice_hello_test (message) VALUES ('Hello from pgslice!')
      `,
    );
    const result = await transaction.any(
      sql.type(z.object({ message: z.string().nullable() }))`
        SELECT message FROM pgslice_hello_test
      `,
    );
    for (const row of result) {
      this.context.stdout.write(`${row.message}\n`);
    }
  }
}
