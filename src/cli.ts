import { Builtins, Cli } from "clipanion";

import { HelloCommand } from "./commands/hello.js";

export function createCli(): Cli {
  const cli = new Cli({
    binaryLabel: "pgslice",
    binaryName: "pgslice",
    binaryVersion: "0.1.0",
  });

  cli.register(Builtins.HelpCommand);
  cli.register(Builtins.VersionCommand);
  cli.register(HelloCommand);

  return cli;
}
