module PgSlice
  module Helpers
    SQL_FORMAT = {
      day: "YYYYMMDD",
      month: "YYYYMM",
      year: "YYYY"
    }

    # ULID epoch start corresponding to 01/01/1970
    DEFAULT_ULID = "00000H5A406P0C3DQMCQ5MV6WQ"

    protected

    # output

    def log(message = nil)
      error message
    end

    def log_sql(message = nil)
      say message
    end

    def abort(message)
      raise Thor::Error, message
    end

    # database connection

    def connection
      @connection ||= begin
        url = options[:url] || ENV["PGSLICE_URL"]
        abort "Set PGSLICE_URL or use the --url option" unless url

        uri = URI.parse(url)
        params = CGI.parse(uri.query.to_s)
        # remove schema
        @schema = Array(params.delete("schema") || "public")[0]
        uri.query = params.any? ? URI.encode_www_form(params) : nil

        ENV["PGCONNECT_TIMEOUT"] ||= "3"
        conn = PG::Connection.new(uri.to_s)
        conn.set_notice_processor do |message|
          say message
        end
        @server_version_num = conn.exec("SHOW server_version_num")[0]["server_version_num"].to_i
        if @server_version_num < 130000
          abort "This version of pgslice requires Postgres 13+"
        end
        conn
      end
    rescue PG::ConnectionBad => e
      abort e.message
    rescue URI::InvalidURIError
      abort "Invalid url"
    end

    def schema
      connection # ensure called first
      @schema
    end

    def execute(query, params = [])
      connection.exec_params(query, params).to_a
    end

    def run_queries(queries)
      connection.transaction do
        execute("SET LOCAL client_min_messages TO warning") unless options[:dry_run]
        log_sql "BEGIN;"
        log_sql
        run_queries_without_transaction(queries)
        log_sql "COMMIT;"
      end
    end

    def run_query(query)
      log_sql query
      unless options[:dry_run]
        begin
          execute(query)
        rescue PG::ServerError => e
          abort "#{e.class.name}: #{e.message}"
        end
      end
      log_sql
    end

    def run_queries_without_transaction(queries)
      queries.each do |query|
        run_query(query)
      end
    end

    def server_version_num
      connection # ensure called first
      @server_version_num
    end

    # helpers

    def sql_date(time, cast, add_cast = true)
      if cast == "timestamptz"
        fmt = "%Y-%m-%d %H:%M:%S UTC"
      else
        fmt = "%Y-%m-%d"
      end
      str = quote(time.strftime(fmt))
      if add_cast
        case cast
        when "date", "timestamptz"
          "#{str}::#{cast}"
        else
          abort "Invalid cast"
        end
      else
        str
      end
    end

    def name_format(period)
      case period.to_sym
      when :day
        "%Y%m%d"
      when :month
        "%Y%m"
      else
        "%Y"
      end
    end

    def partition_date(partition, name_format)
      DateTime.strptime(partition.name.split("_").last, name_format)
    end

    def round_date(date, period)
      date = date.to_date
      case period.to_sym
      when :day
        date
      when :month
        Date.new(date.year, date.month)
      else
        Date.new(date.year)
      end
    end

    def assert_table(table)
      abort "Table not found: #{table}" unless table.exists?
    end

    def assert_no_table(table)
      abort "Table already exists: #{table}" if table.exists?
    end

    def advance_date(date, period, count = 1)
      date = date.to_date
      case period.to_sym
      when :day
        date.next_day(count)
      when :month
        date.next_month(count)
      else
        date.next_year(count)
      end
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def quote(value)
      if value.is_a?(Numeric)
        value
      else
        connection.escape_literal(value)
      end
    end

    def quote_table(table)
      table.quote_table
    end

    # ULID helper methods
    def ulid?(value)
      return false unless value.is_a?(String)
      # Match pure ULIDs or ULIDs with prefixes
      value.match?(/\A[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}\z/) ||
        value.match?(/.*[0123456789ABCDEFGHJKMNPQRSTVWXYZ]{26}\z/)
    end

    def numeric_id?(value)
      value.is_a?(Numeric) || (value.is_a?(String) && value.match?(/\A\d+\z/))
    end

    def id_type(value)
      return :numeric if numeric_id?(value)
      return :ulid if ulid?(value)
      :unknown
    end

    # Factory method to get the appropriate ID handler
    def id_handler(sample_id)
      if ulid?(sample_id)
        UlidHandler.new
      else
        NumericHandler.new
      end
    end

    class NumericHandler
      def min_value
        1
      end

      def predecessor(id)
        id - 1
      end

      def should_continue?(current_id, max_id)
        current_id < max_id
      end

      def batch_count(starting_id, max_id, batch_size)
        ((max_id - starting_id) / batch_size.to_f).ceil
      end

      def batch_where_condition(primary_key, starting_id, batch_size, inclusive = false)
        helpers = PgSlice::CLI.instance
        operator = inclusive ? ">=" : ">"
        "#{helpers.quote_ident(primary_key)} #{operator} #{helpers.quote(starting_id)} AND #{helpers.quote_ident(primary_key)} <= #{helpers.quote(starting_id + batch_size)}"
      end

      def next_starting_id(starting_id, batch_size)
        starting_id + batch_size
      end
    end

    class UlidHandler
      def min_value
        PgSlice::Helpers::DEFAULT_ULID
      end

      def predecessor(id)
        PgSlice::Helpers::DEFAULT_ULID
      end

      def should_continue?(current_id, max_id)
        current_id < max_id
      end

      def batch_count(starting_id, max_id, batch_size)
        nil  # Unknown for ULIDs
      end

      def batch_where_condition(primary_key, starting_id, batch_size, inclusive = false)
        operator = inclusive ? ">=" : ">"
        "#{PG::Connection.quote_ident(primary_key)} #{operator} '#{starting_id}'"
      end

      def next_starting_id(starting_id, batch_size)
        # For ULIDs, we need to get the max ID from the current batch
        # This will be handled in the fill logic
        nil
      end
    end

    def quote_no_schema(table)
      quote_ident(table.name)
    end

    def create_table(name)
      if name.include?(".")
        schema, name = name.split(".", 2)
      else
        schema = self.schema
      end
      Table.new(schema, name)
    end

    def make_index_def(index_def, table)
      index_def.sub(/ ON \S+ USING /, " ON #{quote_table(table)} USING ").sub(/ INDEX .+ ON /, " INDEX ON ") + ";"
    end

    def make_fk_def(fk_def, table)
      "ALTER TABLE #{quote_table(table)} ADD #{fk_def};"
    end

    def make_stat_def(stat_def, table)
      m = /ON (.+) FROM/.match(stat_def)
      # errors on duplicate names, but should be rare
      stat_name = "#{table}_#{m[1].split(", ").map { |v| v.gsub(/\W/i, "") }.join("_")}_stat"
      stat_def.sub(/ FROM \S+/, " FROM #{quote_table(table)}").sub(/ STATISTICS .+ ON /, " STATISTICS #{quote_ident(stat_name)} ON ") + ";"
    end
  end
end
