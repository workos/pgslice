import { describe, expect } from "vitest";
import { sql } from "slonik";
import { z } from "zod";

import { commandTest as test } from "../testing/index.js";
import { DisableRetiredMirroringCommand } from "./disable-retired-mirroring.js";

describe("DisableRetiredMirroringCommand", () => {
  test.scoped({
    commandClass: ({}, use) => use(DisableRetiredMirroringCommand),
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

    test("outputs success message", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      // First enable retired mirroring
      await pgslice.enableMirroring(transaction, {
        table: "posts",
        targetType: "retired",
      });

      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers disabled for posts");
    });

    test("supports legacy command alias", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, {
        table: "posts",
        targetType: "retired",
      });

      const exitCode = await cli.run(
        ["disable_retired_mirroring", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers disabled for posts");
    });

    test("succeeds even when no trigger exists (idempotent)", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers disabled for posts");
    });
    test("keeps the trigger in place but disabled by default", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, {
        table: "posts",
        targetType: "retired",
      });

      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const trigger = await transaction.one(
        sql.type(z.object({ tgenabled: z.string() }))`
          SELECT tgenabled FROM pg_trigger WHERE tgname = 'posts_retired_mirror_trigger'
        `,
      );
      expect(trigger.tgenabled).toBe("D");
    });

    test("drops the trigger and function with --drop", async ({
      cli,
      commandContext,
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, {
        table: "posts",
        targetType: "retired",
      });

      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "posts", "--drop"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain("Retired mirroring triggers dropped for posts");

      const remaining = await transaction.oneFirst(
        sql.type(z.object({ count: z.coerce.number() }))`
          SELECT (
            (SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'posts_retired_mirror_trigger')
            + (SELECT COUNT(*) FROM pg_proc WHERE proname = 'posts_mirror_to_retired')
          )::int AS count
        `,
      );
      expect(remaining).toBe(0);
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
      pgslice,
      transaction,
    }) => {
      await pgslice.enableMirroring(transaction, {
        table: "myschema.posts",
        targetType: "retired",
      });

      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "myschema.posts"],
        commandContext,
      );

      expect(exitCode).toBe(0);
      const output = commandContext.stdout.read()?.toString();
      expect(output).toContain(
        "Retired mirroring triggers disabled for myschema.posts",
      );
    });
  });

  describe("errors", () => {
    test("returns error when source table not found", async ({
      cli,
      commandContext,
    }) => {
      const exitCode = await cli.run(
        ["mirroring", "disable-retired", "nonexistent"],
        commandContext,
      );

      expect(exitCode).toBe(1);
      const output = commandContext.stderr.read()?.toString();
      expect(output).toContain("Table not found: public.nonexistent");
    });
  });
});
