module RS2BQ
  class BigQuerySchemaBuilder
    def initialize(redshift_connection)
      @redshift_connection = redshift_connection
    end

    def big_query_schema_from(redshift_table_name)
      table_schema = RedshiftTableSchema.new(redshift_table_name, @redshift_connection)
      fields = table_schema.columns.map(&:to_big_query_field)
      if fields.empty?
        raise sprintf('Table not found: %s', redshift_table_name.inspect)
      else
        {'fields' => fields}
      end
    end
  end
end
