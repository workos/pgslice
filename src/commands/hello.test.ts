import { expect } from "vitest";
import { sql } from "slonik";
import { commandTest as test } from "../testing/index.js";
import { HelloCommand } from "./hello.js";

test("HelloCommand.perform() runs database statements", async ({
  transaction,
  commandContext,
}) => {
  const command = new HelloCommand();
  command.context = commandContext;

  await command.execute();

  const output = commandContext.stdout.read()?.toString();
  expect(output).toContain("Hello from pgslice!");
  expect(
    await transaction.many(sql.unsafe`SELECT * FROM pgslice_hello_test`),
  ).toEqual([{ id: 1, message: "Hello from pgslice!" }]);
});
