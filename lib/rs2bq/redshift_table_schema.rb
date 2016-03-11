module RS2BQ
  class RedshiftTableSchema
    def initialize(table_name, redshift_connection)
      @table_name = table_name
      @redshift_connection = redshift_connection
    end

    def columns
      @columns ||= @redshift_connection.exec_params(%|SELECT "column", "type", "notnull" FROM "pg_table_def" WHERE "schemaname" = 'public' AND "tablename" = $1|, [@table_name]).map do |row|
        name = row['column']
        type = row['type']
        nullable = row['notnull'] == 'f'
        Column.new(name, type, nullable)
      end
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

      def to_big_query_field
        {
          'name' => @name,
          'type' => big_query_type,
          'mode' => @nullable ? 'NULLABLE' : 'REQUIRED'
        }
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
