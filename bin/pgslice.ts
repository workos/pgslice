#!/usr/bin/env node

import { createCli } from "../src/cli.js";

createCli().runExit(process.argv.slice(2));
