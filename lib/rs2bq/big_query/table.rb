module RS2BQ
  module BigQuery
    class Table
      def initialize(big_query_service, table_data, options={})
        @big_query_service = big_query_service
        @table_data = table_data
        @logger = options[:logger] || NullLogger::INSTANCE
        @thread = options[:thread] || Kernel
      end

      def load(uri, options={})
        poll_interval = options[:poll_interval] || 60
        load_configuration = {}
        load_configuration[:source_uris] = [uri]
        load_configuration[:write_disposition] = options[:allow_overwrite] ? 'WRITE_TRUNCATE' : 'WRITE_EMPTY'
        load_configuration[:create_disposition] = 'CREATE_IF_NEEDED'
        load_configuration[:schema] = options[:schema] if options[:schema]
        load_configuration[:source_format] = 'CSV'
        load_configuration[:field_delimiter] = '\t'
        load_configuration[:quote] = '"'
        load_configuration[:destination_table] = @table_data.table_reference
        job = Google::Apis::BigqueryV2::Job.new(
          configuration: Google::Apis::BigqueryV2::JobConfiguration.new(
            load: Google::Apis::BigqueryV2::JobConfigurationLoad.new(load_configuration)
          )
        )
        job = @big_query_service.insert_job(@table_data.table_reference.project_id, job)
        @logger.info(sprintf('Loading rows from %s to the table %s.%s', uri, @table_data.table_reference.dataset_id, @table_data.table_reference.table_id))
        loop do
          job = @big_query_service.get_job(@table_data.table_reference.project_id, job.job_reference.job_id)
          if job.status && job.status.state == 'DONE'
            if job.status.error_result
              raise job.status.error_result.message
            else
              break
            end
          else
            state = job.status && job.status.state
            @logger.debug(sprintf('Waiting for job %s (status: %s)', job.job_reference.job_id.inspect, state ? state.inspect : 'unknown'))
            @thread.sleep(poll_interval)
          end
        end
        @logger.info('Loading complete')
        nil
      end
    end
  end
end
