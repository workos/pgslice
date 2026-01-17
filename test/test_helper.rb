require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "open3"
require "ulid"

Dotenv.load(".test.env")

PgSlice::CLI.exit_on_failure = false

def use_typescript_port?
  ENV["USE_TYPESCRIPT_PORT"] == "1"
end
