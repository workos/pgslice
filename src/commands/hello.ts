import { Command } from "clipanion";

export class HelloCommand extends Command {
  static override paths = [["hello"]];

  static override usage = Command.Usage({
    description: "A placeholder command to verify the CLI works",
  });

  async execute(): Promise<number> {
    this.context.stdout.write("Hello from pgslice!\n");
    return 0;
  }
}
