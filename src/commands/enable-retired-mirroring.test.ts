import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { EnableRetiredMirroringCommand } from "./enable-retired-mirroring.js";

describe("EnableRetiredMirroringCommand", () => {
  test.scoped({
    commandClass: ({}, use) => use(EnableRetiredMirroringCommand),
  });

  describe("success", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);

      await transaction.query(sql.unsafe`
        CREATE TABLE posts_retired (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
    });

    test("outputs success message", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(
        ["mirroring", "enable-retired", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers enabled for posts");
    });

    test("supports legacy command alias", async ({ cli, commandContext }) => {
      const exitCode = await cli.run(
        ["enable_retired_mirroring", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers enabled for posts");
    });
  });

  describe("schema-qualified tables", () => {
    test.beforeEach(async ({ transaction }) => {
      await transaction.query(sql.unsafe`CREATE SCHEMA myschema`);
      await transaction.query(sql.unsafe`
        CREATE TABLE myschema.posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
      await transaction.query(sql.unsafe`
        CREATE TABLE myschema.posts_retired (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
    });

    test("works with schema-qualified table names", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "enable-retired", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        "Retired mirroring triggers enabled for myschema.posts",
      );
    });
  });

  describe("errors", () => {
    test("returns error when source table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "enable-retired", "nonexistent"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.nonexistent");
    });

    test("returns error when retired table not found", async ({
      cli,
      commandContext,
      transaction,
    }) => {
      await transaction.query(sql.unsafe`
        CREATE TABLE posts (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);

      const exitCode = await cli.run(
        ["mirroring", "enable-retired", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.posts_retired");
    });
  });
});
