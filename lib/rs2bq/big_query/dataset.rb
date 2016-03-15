module RS2BQ
  module BigQuery
    class Dataset
      def initialize(big_query_service, project_id, dataset_id, options={})
        @big_query_service = big_query_service
        @project_id = project_id
        @dataset_id = dataset_id
        @logger = options[:logger] || NullLogger::INSTANCE
      end

      def table(table_name)
        table_data = @big_query_service.get_table(@project_id, @dataset_id, table_name)
        Table.new(@big_query_service, table_data, logger: @logger)
      rescue Google::Apis::ClientError => e
        if e.status_code == 404
          nil
        else
          raise
        end
      end

      def create_table(table_name, options={})
        table_reference = Google::Apis::BigqueryV2::TableReference.new(
          project_id: @project_id,
          dataset_id: @dataset_id,
          table_id: table_name
        )
        if options[:schema]
          fields = options[:schema]['fields'].map { |f| Google::Apis::BigqueryV2::TableFieldSchema.new(name: f['name'], type: f['type'], mode: f['mode']) }
          schema = Google::Apis::BigqueryV2::TableSchema.new(fields: fields)
        end
        table_spec = {}
        table_spec[:table_reference] = table_reference
        table_spec[:schema] = schema if schema
        table_data = Google::Apis::BigqueryV2::Table.new(table_spec)
        table_data = @big_query_service.insert_table(@project_id, @dataset_id, table_data)
        Table.new(@big_query_service, table_data, logger: @logger)
      end
    end
  end
end
