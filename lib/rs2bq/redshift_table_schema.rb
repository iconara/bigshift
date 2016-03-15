module RS2BQ
  class RedshiftTableSchema
    def initialize(table_name, redshift_connection)
      @table_name = table_name
      @redshift_connection = redshift_connection
    end

    def columns
      @columns ||= begin
        rows = @redshift_connection.exec_params(%|SELECT "column", "type", "notnull" FROM "pg_table_def" WHERE "schemaname" = 'public' AND "tablename" = $1|, [@table_name])
        if rows.count == 0
          raise sprintf('Table not found: %s', @table_name.inspect)
        else
          columns = rows.map do |row|
            name = row['column']
            type = row['type']
            nullable = row['notnull'] == 'f'
            Column.new(name, type, nullable)
          end
          columns.sort_by!(&:name)
          columns
        end
      end
    end

    def to_big_query
      Google::Apis::BigqueryV2::TableSchema.new(fields: columns.map(&:to_big_query))
    end

    class Column
      attr_reader :name, :type

      def initialize(name, type, nullable)
        @name = name
        @type = type
        @nullable = nullable
      end

      def nullable?
        @nullable
      end

      def to_big_query
        Google::Apis::BigqueryV2::TableFieldSchema.new(
          name: @name,
          type: big_query_type,
          mode: @nullable ? 'NULLABLE' : 'REQUIRED'
        )
      end

      private

      def big_query_type
        case @type
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
end
