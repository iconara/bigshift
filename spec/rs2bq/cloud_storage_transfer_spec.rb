module RS2BQ
  describe CloudStorageTransfer do
    let :transfer do
      described_class.new(storage_transfer_service, 'my_project', aws_credentials, clock: clock, thread: thread, logger: logger)
    end

    let :storage_transfer_service do
      double(:storage_transfer_service)
    end

    let :aws_credentials do
      {
        'aws_access_key_id' => 'my-aws-access-key-id',
        'aws_secret_access_key' => 'my-aws-secret-access-key',
      }
    end

    let :clock do
      double(:clock)
    end

    let :thread do
      double(:thread)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil)
    end

    let :created_jobs do
      []
    end

    let :job do
      double(:job, name: 'my_job')
    end

    let :transfer_operation do
      double(:operation, done?: true, metadata: {'status' => 'SUCCESS'})
    end

    let :now do
      Time.utc(2016, 3, 11, 19, 2, 0)
    end

    before do
      allow(storage_transfer_service).to receive(:create_transfer_job) do |j|
        created_jobs << j
        allow(job).to receive(:description).and_return(j.description)
        job
      end
      allow(storage_transfer_service).to receive(:list_transfer_operations).and_return(double(operations: [transfer_operation]))
      allow(thread).to receive(:sleep)
    end

    before do
      allow(clock).to receive(:now).and_return(now)
    end

    describe '#copy_to' do
      context 'creates a transfer job that' do
        before do
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', options)
        end

        let :options do
          {}
        end

        it 'has the project ID set' do
          expect(created_jobs.first.project_id).to eq('my_project')
        end

        it 'is enabled' do
          expect(created_jobs.first.status).to eq('ENABLED')
        end

        it 'is scheduled to start as soon as possible, with some margin of error' do
          schedule = created_jobs.first.schedule
          aggregate_failures do
            expect(schedule.schedule_start_date.year).to eq(2016)
            expect(schedule.schedule_start_date.month).to eq(3)
            expect(schedule.schedule_start_date.day).to eq(11)
            expect(schedule.schedule_end_date.year).to eq(2016)
            expect(schedule.schedule_end_date.month).to eq(3)
            expect(schedule.schedule_end_date.day).to eq(11)
            expect(schedule.start_time_of_day.hours).to eq(19)
            expect(schedule.start_time_of_day.minutes).to eq(3)
          end
        end

        context 'when the client does not use UTC' do
          let :now do
            Time.new(2016, 3, 11, 19, 2, 0, '-04:30')
          end

          it 'converts the time to UTC' do
            schedule = created_jobs.first.schedule
            aggregate_failures do
              expect(schedule.schedule_start_date.year).to eq(2016)
              expect(schedule.schedule_start_date.month).to eq(3)
              expect(schedule.schedule_start_date.day).to eq(11)
              expect(schedule.schedule_end_date.year).to eq(2016)
              expect(schedule.schedule_end_date.month).to eq(3)
              expect(schedule.schedule_end_date.day).to eq(11)
              expect(schedule.start_time_of_day.hours).to eq(23)
              expect(schedule.start_time_of_day.minutes).to eq(33)
            end
          end
        end

        it 'copies from the specified location on S3' do
          transfer_spec = created_jobs.first.transfer_spec
          aggregate_failures do
            expect(transfer_spec.aws_s3_data_source.bucket_name).to eq('my-s3-bucket')
            expect(transfer_spec.object_conditions.include_prefixes).to eq(['the/prefix'])
          end
        end

        it 'uses the provided AWS credentials' do
          aws_credentials = created_jobs.first.transfer_spec.aws_s3_data_source.aws_access_key
          aggregate_failures do
            expect(aws_credentials.access_key_id).to eq('my-aws-access-key-id')
            expect(aws_credentials.secret_access_key).to eq('my-aws-secret-access-key')
          end
        end

        it 'copies to the specified GCS bucket' do
          expect(created_jobs.first.transfer_spec.gcs_data_sink.bucket_name).to eq('my-gcs-bucket')
        end

        it 'does not overwrite the destination' do
          expect(created_jobs.first.transfer_spec.transfer_options.overwrite_objects_already_existing_in_sink).to equal(false)
        end

        context 'when the :allow_overwrite option is true' do
          let :options do
            super().merge(allow_overwrite: true)
          end

          it 'allows overwriting files at the destination' do
            expect(created_jobs.first.transfer_spec.transfer_options.overwrite_objects_already_existing_in_sink).to equal(true)
          end
        end
      end

      context 'when given a description' do
        it 'sets the job\'s description to the specified value' do
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar')
          expect(created_jobs.first.description).to eq('foobar')
        end
      end

      context 'submits the transfer job and' do
        it 'looks up the transfer job' do
          operation_name = nil
          filter = nil
          allow(storage_transfer_service).to receive(:list_transfer_operations) do |name, options|
            operation_name = name
            filter = JSON.load(options[:filter])
            double(operations: [transfer_operation])
          end
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar')
          expect(operation_name).to eq('transferOperations')
          expect(filter).to eq('project_id' => 'my_project', 'job_names' => ['my_job'])
        end

        it 'logs that the transfer has started' do
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar')
          expect(logger).to have_received(:info).with('Transferring objects from s3://my-s3-bucket/the/prefix to gs://my-gcs-bucket/the/prefix')
        end

        it 'waits until the transfer job is done' do
          allow(storage_transfer_service).to receive(:list_transfer_operations).and_return(
            double(operations: nil),
            double(operations: []),
            double(operations: []),
            double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})]),
            double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})]),
            double(operations: [double(done?: true, metadata: {'status' => 'SUCCESS'})]),
          )
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar', poll_interval: 13)
          expect(storage_transfer_service).to have_received(:list_transfer_operations).exactly(6).times
          expect(thread).to have_received(:sleep).with(13).exactly(5).times
        end

        context 'when the job changes status' do
          before do
            allow(storage_transfer_service).to receive(:list_transfer_operations).and_return(
              double(operations: nil),
              double(operations: []),
              double(operations: []),
              double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})]),
              double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})]),
              double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})]),
              double(operations: [double(done?: true, metadata: {'status' => 'SUCCESS'})]),
            )
            transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar', poll_interval: 13)
          end

          it 'logs the status when the job is not done' do
            expect(logger).to have_received(:debug).with('Waiting for job "foobar" (name: "my_job", status: unknown)').exactly(3).times
            expect(logger).to have_received(:debug).with('Waiting for job "foobar" (name: "my_job", status: "IN_PROGRESS")').at_least(:once)
          end

          it 'logs the status when the job gets an in-progress status' do
            expect(logger).to have_received(:info).with('Transfer foobar started')
          end

          it 'logs when the job is done' do
            expect(logger).to have_received(:info).with('Transfer foobar complete')
          end
        end

        context 'when the job fails' do
          let :transfer_operation do
            double(done?: true, metadata: {'status' => 'FAILED'})
          end

          it 'raises an error' do
            expect { transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket') }.to raise_error(/Transfer failed/)
          end
        end

        context 'when the job metadata contains counters' do
          let :transfer_operation do
            double(done?: true, metadata: {'status' => 'SUCCESS', 'counters' => counters})
          end

          let :counters do
            {
              'objectsCopiedToSink' => '1106',
              'bytesCopiedToSink' => '33980210508'
            }
          end

          it 'logs statistics about the job when it completes' do
            transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar')
            expect(logger).to have_received(:info).with('Transfer foobar complete, 1106 objects and 31.65 GiB copied')
          end
        end

        context 'when the server responds with an error' do
          before do
            calls = 0
            allow(storage_transfer_service).to receive(:list_transfer_operations) do
              calls += 1
              if calls < 3
                double(operations: [double(done?: false, metadata: {'status' => 'IN_PROGRESS'})])
              elsif calls == 3
                raise Google::Apis::ServerError, 'Bork!'
              else
                double(operations: [double(done?: true, metadata: {'status' => 'SUCCESS'})])
              end
            end
          end

          it 'retries the operation' do
            transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar', poll_interval: 13)
            expect(storage_transfer_service).to have_received(:list_transfer_operations).exactly(4).times
            expect(thread).to have_received(:sleep).with(13).exactly(3).times
          end

          it 'logs the error' do
            transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar', poll_interval: 13)
            expect(logger).to have_received(:debug).with('Error while waiting for job "my_job", will retry: "Bork!" (Google::Apis::ServerError)')
          end

          it 'raises an error after five retries' do
            allow(storage_transfer_service).to receive(:list_transfer_operations).and_raise(Google::Apis::ServerError, 'Bork!')
            expect { transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket') }.to raise_error('Transfer failed: "Bork!" (Google::Apis::ServerError)')
            expect(storage_transfer_service).to have_received(:list_transfer_operations).exactly(5).times
          end
        end
      end
    end
  end
end
