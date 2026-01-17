#!/usr/bin/env node

import { createCli } from "../src/cli.js";

const cli = createCli();
cli.runExit(process.argv.slice(2));
