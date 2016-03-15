module RS2BQ
  class RedshiftUnloader
    def initialize(redshift_connection, aws_credentials, options={})
      @redshift_connection = redshift_connection
      @aws_credentials = aws_credentials
      @allow_overwrite = options[:allow_overwrite]
    end

    def unload_to(table_name, s3_prefix)
      table_schema = RedshiftTableSchema.new(table_name, @redshift_connection)
      credentials = @aws_credentials.map { |pair| pair.join('=') }.join(';')
      select_sql = 'SELECT '
      select_sql << table_schema.columns.map(&:to_sql).join(', ')
      select_sql << %Q< FROM "#{table_name}">
      unload_sql = %Q<UNLOAD ('#{select_sql}')>
      unload_sql << %Q< TO '#{s3_prefix}'>
      unload_sql << %Q< CREDENTIALS '#{credentials}'>
      unload_sql << %q< DELIMITER ','>
      unload_sql << %q< ADDQUOTES>
      unload_sql << %q< ESCAPE>
      unload_sql << %q< ALLOWOVERWRITE> if @allow_overwrite
      @redshift_connection.exec(unload_sql)
    end
  end
end
