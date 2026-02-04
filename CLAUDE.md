# pgslice

This is the repository for `pgslice`, a utility that provides various commands
to manage database table partitioning in Postgres. You should be familiar with
the core functionality as described in the @README.md.

## Structure

- `package.json` - Package metadata, including dependencies.
- `src` - Source code and tests.
- `dist` - Compiled TypeScript artifacts.
- `bin` - CLI entrypoints.

## Guidelines

- We want as much type-safety as possible, so try to avoid type or non-null
  assertions, etc.
- Only add code-comments when the code itself is not unclear or not
  self-describing.
- Code is formatted using Prettier and `npm run format`.
- We don't need to support older versions of Postgres; assume only version 13.x
  or later.
- We don't need to support "trigger-based partitioning". Only native
  Postgres partitioning will be supported.

## Dependencies

- [`slonik`](https://github.com/gajus/slonik) - Postgres client that handles
  connecting to the database and running statements.
  - Avoid using `sq.unsafe`. According to the `slonik` docs, it must not be used
    in production code; let's follow their advice.
  - At times you'll be tempted to invent your own `quoteIdent` type of helper.
    You probably don't need it and combinations of `sql.identifier`, `sql.join`,
    and `sql.fragment` can probably do what you need.
- [`clipanion`](https://github.com/arcanis/clipanion) - CLI parsing library
  where individual commands are represented by classes.
- [`vitest`](https://github.com/vitest-dev/vitest) - Testing framework for both
  unit and end-to-end tests.

## Testing

Note that `vitest` and `npm test` will run in _watch_ mode by default. If you
want a single run, pass the `run` subcommand (example: `npm test -- run`).

## Module Interface

In addition to a CLI interface, `pgslice` also exposes an ECMAScript-compatible
module interface. This means another Node program can install this package in
its `package.json`'s `dependencies`, import it, and gain access to the core
feature set to do things like manage partitions, etc.

The core behavior is abstracted to allow exposure through both the CLI and this
module interface.
