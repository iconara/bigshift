module RS2BQ
  class RedshiftUnloader
    def initialize(redshift_connection, aws_credentials)
      @redshift_connection = redshift_connection
      @aws_credentials = aws_credentials
    end

    def unload_to(table_name, s3_prefix)
      table_schema = RedshiftTableSchema.new(table_name, @redshift_connection)
      credentials = @aws_credentials.map { |pair| pair.join('=') }.join(';')
      select_sql = 'SELECT '
      select_sql << table_schema.columns.map { |c| %<"#{c.name}"> }.join(', ')
      select_sql << %Q< FROM "#{table_name}">
      unload_sql = %Q<UNLOAD ('#{select_sql}')>
      unload_sql << %Q< TO '#{s3_prefix}'>
      unload_sql << %Q< CREDENTIALS '#{credentials}'>
      unload_sql << %q< DELIMITER ','>
      unload_sql << %q< ADDQUOTES>
      unload_sql << %q< ESCAPE>
      @redshift_connection.exec(unload_sql)
    end
  end
end
