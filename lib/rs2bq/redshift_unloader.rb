module RS2BQ
  class RedshiftUnloader
    def initialize(redshift_connection, aws_credentials, options={})
      @redshift_connection = redshift_connection
      @aws_credentials = aws_credentials
      @logger = options[:logger] || NullLogger::INSTANCE
    end

    def unload_to(table_name, s3_uri, options={})
      table_schema = RedshiftTableSchema.new(table_name, @redshift_connection)
      credentials = @aws_credentials.map { |pair| pair.join('=') }.join(';')
      select_sql = 'SELECT '
      select_sql << table_schema.columns.map(&:to_sql).join(', ')
      select_sql << %Q< FROM "#{table_name}">
      select_sql.gsub!('\'') { |s| '\\\'' }
      unload_sql = %Q<UNLOAD ('#{select_sql}')>
      unload_sql << %Q< TO '#{s3_uri}'>
      unload_sql << %Q< CREDENTIALS '#{credentials}'>
      unload_sql << %q< DELIMITER '\t'>
      unload_sql << %q< ESCAPE>
      unload_sql << %q< ALLOWOVERWRITE> if options[:allow_overwrite]
      @logger.info(sprintf('Unloading Redshift table %s to %s', table_name, s3_uri))
      @redshift_connection.exec(unload_sql)
      @logger.info(sprintf('Unload of %s complete', table_name))
    end
  end
end
