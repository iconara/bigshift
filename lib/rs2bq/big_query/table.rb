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
        poll_interval = options[:poll_interval] || DEFAULT_POLL_INTERVAL
        load_configuration = {}
        load_configuration[:source_uris] = [uri]
        load_configuration[:write_disposition] = options[:allow_overwrite] ? 'WRITE_TRUNCATE' : 'WRITE_EMPTY'
        load_configuration[:create_disposition] = 'CREATE_IF_NEEDED'
        load_configuration[:schema] = options[:schema] if options[:schema]
        load_configuration[:source_format] = 'CSV'
        load_configuration[:field_delimiter] = '\t'
        load_configuration[:destination_table] = @table_data.table_reference
        job = Google::Apis::BigqueryV2::Job.new(
          configuration: Google::Apis::BigqueryV2::JobConfiguration.new(
            load: Google::Apis::BigqueryV2::JobConfigurationLoad.new(load_configuration)
          )
        )
        job = @big_query_service.insert_job(@table_data.table_reference.project_id, job)
        @logger.info(sprintf('Loading rows from %s to the table %s.%s', uri, @table_data.table_reference.dataset_id, @table_data.table_reference.table_id))
        started = false
        loop do
          job = @big_query_service.get_job(@table_data.table_reference.project_id, job.job_reference.job_id)
          if job.status && job.status.state == 'DONE'
            if job.status.errors.nil? || job.status.errors.empty?
              break
            else
              job.status.errors.each do |error|
                message = %<Load error: "#{error.message}">
                if error.location
                  file, line, field = error.location.split('/').map { |s| s.split(':').last.strip }
                  message << " at file #{file}, line #{line}"
                  message << ", field #{field}" if field
                end
                @logger.debug(message)
              end
              raise job.status.error_result.message
            end
          else
            state = job.status && job.status.state
            if state == 'RUNNING' && !started
              @logger.info('Loading started')
              started = true
            else
              @logger.debug(sprintf('Waiting for job %s (status: %s)', job.job_reference.job_id.inspect, state ? state.inspect : 'unknown'))
            end
            @thread.sleep(poll_interval)
          end
        end
        report_complete(job)
        nil
      end

      private

      DEFAULT_POLL_INTERVAL = 30

      def report_complete(job)
        statistics = job.statistics.load
        input_size = statistics.input_file_bytes.to_f/2**30
        output_size = statistics.output_bytes.to_f/2**30
        @logger.info(sprintf('Loading complete, %.2f GiB loaded from %s files, %s rows created, table size %.2f GiB', input_size, statistics.input_files, statistics.output_rows, output_size))
      end
    end
  end
end
