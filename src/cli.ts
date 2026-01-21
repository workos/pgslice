import { Builtins, Cli } from "clipanion";

import { HelloCommand } from "./commands/hello.js";
import { PrepCommand } from "./commands/prep.js";

export function createCli(): Cli {
  const cli = new Cli({
    binaryLabel: "pgslice",
    binaryName: "pgslice",
    binaryVersion: "0.1.0",
  });

  cli.register(Builtins.HelpCommand);
  cli.register(Builtins.VersionCommand);
  cli.register(HelloCommand);
  cli.register(PrepCommand);

  return cli;
}
