module BigShift
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
        double(:load_job, job_reference: double(job_id: 'my_job_id'), status: double(state: 'DONE', errors: []), statistics: double(load: statistics))
      end

      let :statistics do
        double(:statistics, input_file_bytes: '23618287', input_files: '20', output_bytes: '24870344', output_rows: '41470')
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

          context 'when the :max_bad_records option is specified' do
            let :options do
              super().merge(max_bad_records: 17)
            end

            it 'sets the corresponding load parameter' do
              expect(load_configuration.max_bad_records).to eq(17)
            end
          end
        end

        context 'submits the load job and' do
          def create_job(job_id, status)
            s = status.nil? ? nil : double(state: status, errors: nil)
            double(:load_job, job_reference: double(job_id: job_id), status: s, statistics: double(load: statistics))
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
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'DONE'),
            )
            table.load('my_uri', poll_interval: 13)
            expect(thread).to have_received(:sleep).with(13).exactly(3).times
          end

          it 'logs statistics when the job is done' do
            table.load('my_uri')
            expect(logger).to have_received(:info).with('Loading complete, 0.02 GiB loaded from 20 files, 41470 rows created, table size 0.02 GiB')
          end

          it 'logs the status when the job is not done' do
            allow(big_query_service).to receive(:get_job).and_return(
              create_job('my_job_id', nil),
              create_job('my_job_id', 'PENDING'),
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'DONE'),
            )
            table.load('my_uri')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: unknown)')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: "PENDING")')
            expect(logger).to have_received(:debug).with('Waiting for job "my_job_id" (status: "RUNNING")')
          end

          it 'logs when the job starts' do
            allow(big_query_service).to receive(:get_job).and_return(
              create_job('my_job_id', nil),
              create_job('my_job_id', 'PENDING'),
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'RUNNING'),
              create_job('my_job_id', 'DONE'),
            )
            table.load('my_uri')
            expect(logger).to have_received(:info).with('Loading started')
          end

          context 'when the job fails' do
            let :load_job do
              double(:load_job, job_reference: double(job_id: 'my_job_id'), status: double(state: 'DONE', errors: errors, error_result: errors.first))
            end

            let :errors do
              [
                double(message: 'Bad thing', reason: 'invalid', location: 'File: 0 / Line:5 / Field:18'),
                double(message: 'Not correct', reason: 'invalid', location: 'File: 1 / Line:6'),
                double(message: 'Do better', reason: 'invalid', location: 'File: 2 / Line:7 / Field:20'),
              ]
            end

            it 'raises an error' do
              expect { table.load('my_uri') }.to raise_error(/Bad thing/)
            end

            it 'logs the errors' do
              table.load('my_uri') rescue nil
              aggregate_failures do
                expect(logger).to have_received(:debug).with('Load error: "Bad thing" in File: 0 / Line:5 / Field:18')
                expect(logger).to have_received(:debug).with('Load error: "Not correct" in File: 1 / Line:6')
                expect(logger).to have_received(:debug).with('Load error: "Do better" in File: 2 / Line:7 / Field:20')
              end
            end

            context 'and the error does not have a location' do
              let :errors do
                [
                  double(message: 'Bad thing', reason: 'invalid', location: nil),
                ]
              end

              it 'logs the error without location' do
                table.load('my_uri') rescue nil
                expect(logger).to have_received(:debug).with('Load error: "Bad thing"')
              end
            end
          end
        end
      end
    end
  end
end
