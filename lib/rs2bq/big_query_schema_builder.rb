module RS2BQ
  class BigQuerySchemaBuilder
    def initialize(redshift_connection)
      @redshift_connection = redshift_connection
    end

    def big_query_schema_from(redshift_table_name)
      rows = @redshift_connection.exec_params(%|SELECT "column", "type", "notnull" FROM "pg_table_def" WHERE "schemaname" = 'public' AND "tablename" = $1|, [redshift_table_name])
      fields = rows.map do |row|
        type = to_big_query_type(row['type'])
        mode = row['notnull'] == 't' ? 'REQUIRED' : 'NULLABLE'
        {'name' => row['column'], 'type' => type, 'mode' => mode}
      end
      if fields.empty?
        raise sprintf('Table not found: %s', redshift_table_name.inspect)
      else
        {'fields' => fields}
      end
    end

    private

    def to_big_query_type(type)
      case type
      when /^character/, /^numeric/, 'date', /^timestamp/ then 'STRING'
      when /int/ then 'INTEGER'
      when 'boolean' then 'BOOLEAN'
      when /^double/, 'real' then 'FLOAT'
      else
        raise sprintf('Unsupported column type: %s', type.inspect)
      end
    end
  end
end
