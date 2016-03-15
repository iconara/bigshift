module RS2BQ
  module BigQuery
    describe Table do
      let :table do
        described_class.new(big_query_service, table_data, logger: logger, thread: thread)
      end

      let :big_query_service do
        double(:big_query_service)
      end

      let :table_data do
        double(:table_data, table_reference: table_reference)
      end

      let :table_reference do
        double(:table_reference, project_id: 'my_project', dataset_id: 'my_dataset', table_id: 'my_table')
      end

      let :load_job do
        double(:load_job, job_reference: double(job_id: 'my_job_id'), status: double(state: 'DONE', error_result: nil))
      end

      let :logger do
        double(:logger, debug: nil, info: nil, warn: nil)
      end

      let :thread do
        double(:thread)
      end

      let :inserted_jobs do
        []
      end

      before do
        allow(big_query_service).to receive(:insert_job) do |_, j|
          inserted_jobs << j
          load_job
        end
        allow(big_query_service).to receive(:get_job).with('my_project', 'my_job_id').and_return(load_job)
        allow(thread).to receive(:sleep)
      end

      describe '#load' do
        let :options do
          {}
        end

        context 'creates a load job that' do
          let :load_configuration do
            inserted_jobs.first.configuration.load
          end

          before do
            table.load('some_uri', options)
          end

          it 'is in the same project as the table' do
            expect(big_query_service).to have_received(:insert_job).with('my_project', anything)
          end

          it 'targets the table' do
            aggregate_failures do
              expect(load_configuration.destination_table.project_id).to eq(table_data.table_reference.project_id)
              expect(load_configuration.destination_table.dataset_id).to eq(table_data.table_reference.dataset_id)
              expect(load_configuration.destination_table.table_id).to eq(table_data.table_reference.table_id)
            end
          end

          it 'loads from the specified URI' do
            expect(load_configuration.source_uris).to eq(['some_uri'])
          end

          it 'requires an empty table' do
            expect(load_configuration.write_disposition).to eq('WRITE_EMPTY')
          end

          it 'creates the destination table if necessary' do
            expect(load_configuration.create_disposition).to eq('CREATE_IF_NEEDED')
          end

          it 'is configured for TSV data' do
            aggregate_failures do
              expect(load_configuration.source_format).to eq('CSV')
              expect(load_configuration.field_delimiter).to eq('\t')
              expect(load_configuration.quote).to eq('"')
            end
          end

          context 'when the :allow_overwrite option is true' do
            let :options do
              super().merge(allow_overwrite: true)
            end

            it 'truncates the destination table' do
              expect(load_configuration.write_disposition).to eq('WRITE_TRUNCATE')
            end
          end

          context 'when the :schema option is specified' do
            let :options do
              super().merge(schema: schema)
            end

            let :schema do
              double(:schema)
            end

            it 'uses the specified schema' do
              expect(load_configuration.schema).to eq(schema)
            end
          end

          context 'when the :schema option is not specified' do
            it 'does not specify a schema' do
              expect(load_configuration.schema).to be_nil
            end
          end
        end

        context 'submits the load job and' do
          def create_job(job_id, status)
            s = status.nil? ? nil : double(state: status, error_result: nil)
            double(:load_job, job_reference: double(job_id: job_id), status: s)
          end

          it 'looks up the job' do
            table.load('my_uri')
            expect(big_query_service).to have_received(:get_job).with('my_project', 'my_job_id')
          end

          it 'logs that the load has started' do
            table.load('my_uri')
            expect(logger).to have_received(:info).with('Loading rows from my_uri to the table my_dataset.my_table')
          end

          it 'waits until the job is done' do
            allow(big_query_service).to receive(:get_job).and_return(
              create_job('my_job_id', nil),
              create_job('my_job_id', 'PENDING'),
              create_job('my_job_id', 'IN_PROGRESS'),
              create_job('my_job_id', 'DONE'),
            )
            table.load('my_uri', poll_interval: 13)
            expect(thread).to have_received(:sleep).with(13).exactly(3).times
          end

          it 'logs when the job is done' do
            table.load('my_uri')
            expect(logger).to have_received(:info).with('Loading complete')
          end

          it 'logs the status when the job is not done' do
            allow(big_query_service).to receive(:get_job).and_return(
              create_job('my_job_id', nil),
              create_job('my_job_id', 'PENDING'),
              create_job('my_job_id', 'IN_PROGRESS'),
              create_job('my_job_id', 'DONE'),
            )
            table.load('my_uri')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: unknown)')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: "PENDING")')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: "IN_PROGRESS")')
          end

          context 'when the job fails' do
            let :load_job do
              double(:load_job, job_reference: double(job_id: 'my_job_id'), status: double(state: 'DONE', error_result: double(message: 'Bork snork!')))
            end

            it 'raises an error' do
              expect { table.load('my_uri') }.to raise_error(/Bork snork!/)
            end
          end
        end
      end
    end
  end
end
