import { describe, expect } from "vitest";
import { sql } from "slonik";

import { commandTest as test } from "../testing/index.js";
import { DisableMirroringCommand } from "./disable-mirroring.js";

describe("DisableMirroringCommand", () => {
  test.scoped({ commandClass: ({}, use) => use(DisableMirroringCommand) });

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
        CREATE TABLE posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL,
          created_at DATE NOT NULL
        )
      `);
    });

    test("outputs success message", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      // First enable mirroring
      await pgslice.enableMirroring(transaction, { table: "posts" });

      const exitCode = await cli.run(
        ["mirroring", "disable", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Mirroring triggers disabled for posts");
    });

    test("supports legacy command alias", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "posts" });

      const exitCode = await cli.run(
        ["disable_mirroring", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Mirroring triggers disabled for posts");
    });

    test("succeeds even when no trigger exists (idempotent)", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "disable", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Mirroring triggers disabled for posts");
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
        CREATE TABLE myschema.posts_intermediate (
          id SERIAL PRIMARY KEY,
          title TEXT NOT NULL
        )
      `);
    });

    test("works with schema-qualified table names", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, { table: "myschema.posts" });

      const exitCode = await cli.run(
        ["mirroring", "disable", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        "Mirroring triggers disabled for myschema.posts",
      );
    });
  });

  describe("errors", () => {
    test("returns error when source table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "disable", "nonexistent"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.nonexistent");
    });
  });
});
