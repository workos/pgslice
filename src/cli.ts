import { Builtins, Cli } from "clipanion";

import { AddPartitionsCommand } from "./commands/add-partitions.js";
import { AnalyzeCommand } from "./commands/analyze.js";
import { DisableMirroringCommand } from "./commands/disable-mirroring.js";
import { DisableRetiredMirroringCommand } from "./commands/disable-retired-mirroring.js";
import { EnableMirroringCommand } from "./commands/enable-mirroring.js";
import { EnableRetiredMirroringCommand } from "./commands/enable-retired-mirroring.js";
import { FillCommand } from "./commands/fill.js";
import { PrepCommand } from "./commands/prep.js";
import { StatusCommand } from "./commands/status.js";
import { SwapCommand } from "./commands/swap.js";
import { SynchronizeCommand } from "./commands/synchronize.js";
import { UnprepCommand } from "./commands/unprep.js";
import { UnswapCommand } from "./commands/unswap.js";

export function createCli(): Cli {
  const cli = new Cli({
    binaryLabel: "pgslice",
    binaryName: "pgslice",
    binaryVersion: "0.1.0",
  });

  cli.register(Builtins.HelpCommand);
  cli.register(Builtins.VersionCommand);
  cli.register(AddPartitionsCommand);
  cli.register(AnalyzeCommand);
  cli.register(DisableMirroringCommand);
  cli.register(DisableRetiredMirroringCommand);
  cli.register(EnableMirroringCommand);
  cli.register(EnableRetiredMirroringCommand);
  cli.register(FillCommand);
  cli.register(PrepCommand);
  cli.register(StatusCommand);
  cli.register(SwapCommand);
  cli.register(SynchronizeCommand);
  cli.register(UnprepCommand);
  cli.register(UnswapCommand);

  return cli;
}
