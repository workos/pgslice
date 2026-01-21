import { PassThrough } from "node:stream";
import { Context } from "../commands/base.js";
import { pgsliceTest } from "./pgslice.js";

export const commandTest = pgsliceTest.extend<{
  commandContext: Context & { stdout: PassThrough; stderr: PassThrough };
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
});
