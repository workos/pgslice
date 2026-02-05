import dotenv from "dotenv";
import { defineConfig } from "vitest/config";

dotenv.config({ path: ".test.env" });

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts"],
    environment: "node",
  },
});
