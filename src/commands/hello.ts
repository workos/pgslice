import { Command } from "clipanion";
import { BaseCommand } from "./base.js";

export class HelloCommand extends BaseCommand {
  static override paths = [["hello"]];

  static override usage = Command.Usage({
    description: "A placeholder command to verify the CLI works",
  });

  async execute(): Promise<number> {
    this.context.stdout.write("Hello from pgslice!\n");
    return 0;
  }
}
