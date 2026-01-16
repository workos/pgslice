# Porting `pgslice`

This is the repository of the `pgslice`, a utility that provides various
commands to manage database table partitioning in Postgres. You should be
familiar with the core functionality as described in the @README.md.

However, this is about to change. The goal with this repository is actually to
_port_ this program from Ruby to TypeScript.

## Structure

The following section describes how the repository layout is structured in terms
of the two implementations that will live alongside each other during the
porting process.

### Ruby Implementation

The following files and folders may or may not still exist, but if they do, they
belong to the original Ruby implementation:

* `Dockerfile` - A portable image to allow running `pgslice` with a compatible Ruby version.
* `Gemfile` - Ruby dependencies
* `Rakefile` - Contains tasks for developing the Ruby version, like running tests.
* `docs` - Files related to documentation of the Ruby implementation's gem release process.
* `exe` - The Ruby gem's CLI entrypoint.
* `lib` - The code for the Ruby implementation.
* `pgslice.gemspec` - The `pgslice` Ruby gem specification, used when published.
* `test` - The original Ruby gem's test suite.

All of these files should be examined and carefully understood to help inform
the porting process. `pgslice` is a battle-tested tool and we want to bring that
experience into the TypeScript version as we port it as much as we can.

### TypeScript Implementation

In general, file structure for the TypeScript version shouldn't overlap much, if
at all, with the old Ruby implementation. Lucky for us! As the porting process
progresses, you can expect to find these new files in the following places:

* `package.json` - TypeScript package metadata, including dependencies.
* `src` - Source code and tests for the new version.
* `dist` - Contains the compiled TypeScript artifacts.
* `bin` - Contains entrypoints for the Typescript's CLI.

Some general guidelines for the TypeScript version:

* We want as much type-safety as possible, so try to avoid type or non-null
  assertions, etc.
* Only add code-comments when the code itself is not unclear or not
  self-describing.

This new version will use the following node packages:

* [`slonik`](https://github.com/gajus/slonik) - Postgres client that will handle
  connecting to the database and running statements.
* [`clipanion`](https://github.com/arcanis/clipanion) - CLI parsing library with
  similar semantics to `thor` from the Ruby version, but individual commands
  are represented by classes.

## Parity

As mentioned above, we can use the original Ruby source code as a guide to
implementing the TypeScript version. As each command is ported over, the
implementation of the old command (generally found under `lib/pgslice/cli`)
should be audited.

The original Ruby test suite found in `test/pgslice_test.rb` is very high level
and exercises behavior from the "outside", as in it runs the `pgslice` CLI
instead of attempting to unit test every module. This means we will eventually
patch this test suite so that it can invoke either the Ruby implementation _or_
the new TypeScript implementation, allowing it to serve as a regression test
suite.

## New Features

In addition to a CLI interface like the old `pgslice`, the TypeScript version
will also expose an ECMAScript-compatible module interface. Meaning, for
example, another Node program could install this new version in its
`package.json`'s `dependencies`, import it, and then gain access to the core
feature set to do things like manage partitions, etc.

This means that we should strive to abstract the core behavior a bit more than
the original Ruby implementation did, allowing us to expose it in both the CLI
and this new module interface.
