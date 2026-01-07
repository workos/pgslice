module PgSlice
  class CLI
    desc "swap TABLE", "Swap the intermediate table with the original table"
    option :lock_timeout, default: "5s", desc: "Lock timeout"
    def swap(table)
      table = create_table(table)
      intermediate_table = table.intermediate_table
      retired_table = table.retired_table

      assert_table(table)
      assert_table(intermediate_table)
      assert_no_table(retired_table)

      queries = []

      # Set lock timeout
      queries << "SET LOCAL lock_timeout = #{quote(options[:lock_timeout])};"

      # Drop the mirror trigger created by enable_mirroring before swap
      queries.concat(disable_mirroring_trigger_queries(table))

      # Swap the tables
      queries << "ALTER TABLE #{quote_table(table)} RENAME TO #{quote_no_schema(retired_table)};"
      queries << "ALTER TABLE #{quote_table(intermediate_table)} RENAME TO #{quote_no_schema(table)};"

      # Update sequence ownership
      table.sequences.each do |sequence|
        queries << "ALTER SEQUENCE #{quote_ident(sequence["sequence_schema"])}.#{quote_ident(sequence["sequence_name"])} OWNED BY #{quote_table(table)}.#{quote_ident(sequence["related_column"])};"
      end

      # Create the retired mirroring trigger after swap
      # Note: After swap, table.name refers to the new main table (formerly intermediate)
      # and retired_table refers to the old main table (formerly the original table)
      queries.concat(enable_retired_mirroring_trigger_queries(table))

      run_queries(queries)
    end
  end
end
