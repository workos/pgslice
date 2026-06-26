import dotenv from "dotenv";
import { defineConfig } from "vitest/config";

dotenv.config({ path: ".test.env" });

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    environment: "node",
    coverage: {
      provider: "v8",
      reporter: ["text", "html"],
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/testing/**", "dist/**"],
      // Measured, not gated: CI enforces no coverage thresholds.
    },
  },
});
