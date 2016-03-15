module RS2BQ
  class CloudStorageTransfer
    def initialize(storage_transfer_service, project_id, aws_credentials, options={})
      @storage_transfer_service = storage_transfer_service
      @project_id = project_id
      @aws_credentials = aws_credentials
      @clock = options[:clock] || Time
      @thread = options[:thread] || Kernel
      @logger = options[:logger] || NullLogger::INSTANCE
    end

    def copy_to_cloud_storage(s3_bucket, s3_path_prefix, cloud_storage_bucket, options={})
      poll_interval = options[:poll_interval] || DEFAULT_POLL_INTERVAL
      transfer_job = create_transfer_job(s3_bucket, s3_path_prefix, cloud_storage_bucket, options[:description], options[:allow_overwrite])
      transfer_job = @storage_transfer_service.create_transfer_job(transfer_job)
      @logger.info(sprintf('Transferring objects from s3://%s/%s to gs://%s/%s', s3_bucket, s3_path_prefix, cloud_storage_bucket, s3_path_prefix))
      await_completion(transfer_job, poll_interval)
      nil
    end

    private

    DEFAULT_POLL_INTERVAL = 30

    def create_transfer_job(s3_bucket, s3_path_prefix, cloud_storage_bucket, description, allow_overwrite)
      now = @clock.now.utc
      Google::Apis::StoragetransferV1::TransferJob.new(
        description: description,
        project_id: @project_id,
        status: 'ENABLED',
        schedule: Google::Apis::StoragetransferV1::Schedule.new(
          schedule_start_date: Google::Apis::StoragetransferV1::Date.new(year: now.year, month: now.month, day: now.day),
          schedule_end_date: Google::Apis::StoragetransferV1::Date.new(year: now.year, month: now.month, day: now.day),
          start_time_of_day: Google::Apis::StoragetransferV1::TimeOfDay.new(hours: now.hour, minutes: now.min + 1)
        ),
        transfer_spec: Google::Apis::StoragetransferV1::TransferSpec.new(
          aws_s3_data_source: Google::Apis::StoragetransferV1::AwsS3Data.new(
            bucket_name: s3_bucket,
            aws_access_key: Google::Apis::StoragetransferV1::AwsAccessKey.new(
              access_key_id: @aws_credentials['aws_access_key_id'],
              secret_access_key: @aws_credentials['aws_secret_access_key'],
            )
          ),
          gcs_data_sink: Google::Apis::StoragetransferV1::GcsData.new(
            bucket_name: cloud_storage_bucket
          ),
          object_conditions: Google::Apis::StoragetransferV1::ObjectConditions.new(
            include_prefixes: [s3_path_prefix]
          ),
          transfer_options: Google::Apis::StoragetransferV1::TransferOptions.new(
            overwrite_objects_already_existing_in_sink: !!allow_overwrite
          )
        )
      )
    end

    def await_completion(transfer_job, poll_interval)
      started = false
      loop do
        operation = nil
        failures = 0
        begin
          operations_response = @storage_transfer_service.list_transfer_operations('transferOperations', filter: JSON.dump({project_id: @project_id, job_names: [transfer_job.name]}))
          operation = operations_response.operations && operations_response.operations.first
        rescue Google::Apis::ServerError => e
          failures += 1
          if failures < 5
            @logger.debug(sprintf('Error while waiting for job %s, will retry: %s (%s)', transfer_job.name.inspect, e.message.inspect, e.class.name))
            @thread.sleep(poll_interval)
            retry
          else
            raise sprintf('Transfer failed: %s (%s)', e.message.inspect, e.class.name)
          end
        end
        if operation && operation.done?
          handle_completion(transfer_job, operation)
          break
        else
          status = operation && operation.metadata && operation.metadata['status']
          if status == 'IN_PROGRESS' && !started
            @logger.info(sprintf('Transfer %s started', transfer_job.description))
            started = true
          else
            @logger.debug(sprintf('Waiting for job %s (name: %s, status: %s)', transfer_job.description.inspect, transfer_job.name.inspect, status ? status.inspect : 'unknown'))
          end
          @thread.sleep(poll_interval)
        end
      end
    end

    def handle_completion(transfer_job, operation)
      if operation.metadata['status'] == 'FAILED'
        raise 'Transfer failed'
      else
        message = sprintf('Transfer %s complete', transfer_job.description)
        if (counters = operation.metadata['counters'])
          size_in_gib = counters['bytesCopiedToSink'].to_f / 2**30
          message << sprintf(', %s objects and %.1f GiB copied', counters['objectsCopiedToSink'], size_in_gib)
        end
        @logger.info(message)
      end
    end
  end
end
