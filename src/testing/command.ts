import { PassThrough } from "node:stream";
import { Builtins, Cli, CommandClass } from "clipanion";

import { Context } from "../commands/base.js";
import { pgsliceTest } from "./pgslice.js";

export const commandTest = pgsliceTest.extend<{
  commandClass: CommandClass;
  commandContext: Context & { stdout: PassThrough; stderr: PassThrough };
  cli: Cli;
}>({
  commandContext: async ({ pgslice }, use) => {
    const stdout = new PassThrough();
    const stderr = new PassThrough();

    use({
      stdin: process.stdin,
      stdout,
      stderr,
      env: process.env,
      colorDepth: 1,
      pgslice,
    });
  },

  commandClass: ({}, use) => use(Builtins.HelpCommand),

  cli: async ({ commandClass }, use) => {
    const cli = new Cli({
      binaryLabel: "pgslice-test",
      binaryName: "pgslice-test",
      binaryVersion: "0.0.0",
    });

    cli.register(commandClass);

    use(cli);
  },
});
