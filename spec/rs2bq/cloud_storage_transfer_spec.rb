module RS2BQ
  describe CloudStorageTransfer do
    let :transfer do
      described_class.new(storage_transfer_service, 'my_project', aws_credentials, clock: clock, thread: thread)
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

    let :created_jobs do
      []
    end

    let :job do
      double(:job, name: 'my_job')
    end

    let :transfer_operations do
      [
        double(:operation, done?: true)
      ]
    end

    before do
      allow(storage_transfer_service).to receive(:create_transfer_job) do |j|
        created_jobs << j
        job
      end
      allow(storage_transfer_service).to receive(:list_transfer_operations).and_return(double(operations: transfer_operations))
      allow(thread).to receive(:sleep)
    end

    before do
      allow(clock).to receive(:now).and_return(double(:now, year: 2016, month: 3, day: 11, hour: 19, min: 2))
    end

    describe '#copy_to' do
      context 'creates a transfer job that' do
        before do
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket')
        end

        it 'has the project ID set' do
          expect(created_jobs.first.project_id).to eq('my_project')
        end

        it 'is enabled' do
          expect(created_jobs.first.status).to eq('ENABLED')
        end

        it 'is scheduled to start immediately' do
          schedule = created_jobs.first.schedule
          aggregate_failures do
            expect(schedule.schedule_start_date.year).to eq(2016)
            expect(schedule.schedule_start_date.month).to eq(3)
            expect(schedule.schedule_start_date.day).to eq(11)
            expect(schedule.schedule_end_date.year).to eq(2016)
            expect(schedule.schedule_end_date.month).to eq(3)
            expect(schedule.schedule_end_date.day).to eq(11)
            expect(schedule.start_time_of_day.hours).to eq(19)
            expect(schedule.start_time_of_day.minutes).to eq(2)
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
          expect(created_jobs.first.transfer_spec.transfer_options.overwrite_objects_already_existing_in_sink).to be_falsy
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
            double(operations: transfer_operations)
          end
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar')
          expect(operation_name).to eq('transferOperations')
          expect(filter).to eq('project_id' => 'my_project', 'job_names' => ['my_job'])
        end

        it 'waits until the transfer job is done' do
          allow(storage_transfer_service).to receive(:list_transfer_operations).and_return(
            double(operations: nil),
            double(operations: []),
            double(operations: []),
            double(operations: [double(done?: false)]),
            double(operations: [double(done?: false)]),
            double(operations: [double(done?: true)]),
          )
          transfer.copy_to_cloud_storage('my-s3-bucket', 'the/prefix', 'my-gcs-bucket', description: 'foobar', poll_interval: 13)
          expect(storage_transfer_service).to have_received(:list_transfer_operations).exactly(6).times
          expect(thread).to have_received(:sleep).with(13).exactly(5).times
        end
      end
    end
  end
end
