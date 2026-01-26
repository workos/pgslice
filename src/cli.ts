import { Builtins, Cli } from "clipanion";

import { AddPartitionsCommand } from "./commands/add-partitions.js";
import { DisableMirroringCommand } from "./commands/disable-mirroring.js";
import { EnableMirroringCommand } from "./commands/enable-mirroring.js";
import { FillCommand } from "./commands/fill.js";
import { PrepCommand } from "./commands/prep.js";
import { SwapCommand } from "./commands/swap.js";
import { SynchronizeCommand } from "./commands/synchronize.js";

export function createCli(): Cli {
  const cli = new Cli({
    binaryLabel: "pgslice",
    binaryName: "pgslice",
    binaryVersion: "0.1.0",
  });

  cli.register(Builtins.HelpCommand);
  cli.register(Builtins.VersionCommand);
  cli.register(AddPartitionsCommand);
  cli.register(DisableMirroringCommand);
  cli.register(EnableMirroringCommand);
  cli.register(FillCommand);
  cli.register(PrepCommand);
  cli.register(SwapCommand);
  cli.register(SynchronizeCommand);

  return cli;
}
