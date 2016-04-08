module BigShift
  module BigQuery
    describe Dataset do
      let :dataset do
        described_class.new(big_query_service, 'my_project', 'my_dataset')
      end

      let :big_query_service do
        double(:big_query_service)
      end

      let :table do
        double(:table)
      end

      before do
        allow(big_query_service).to receive(:insert_table)
        allow(big_query_service).to receive(:get_table).with('my_project', 'my_dataset', 'my_table').and_return(table)
      end

      describe '#table' do
        it 'returns the specified table' do
          expect(dataset.table('my_table')).to be_a(Table)
        end

        context 'when the table does not exist' do
          before do
            allow(big_query_service).to receive(:get_table).and_raise(Google::Apis::ClientError.new('Bork', status_code: 404))
          end

          it 'returns nil' do
            expect(dataset.table('my_table')).to be_nil
          end
        end

        context 'when an non-not-found error is raised' do
          before do
            allow(big_query_service).to receive(:get_table).and_raise(Google::Apis::ClientError.new('Bork', status_code: 500))
          end

          it 're-raises the error' do
            expect { dataset.table('my_table') }.to raise_error(Google::Apis::ClientError)
          end
        end
      end

      describe '#create_table' do
        it 'creates the table' do
          table_reference = nil
          allow(big_query_service).to receive(:insert_table) do |_, _, table|
            table_reference = table.table_reference
          end
          dataset.create_table('my_table')
          aggregate_failures do
            expect(table_reference.project_id).to eq('my_project')
            expect(table_reference.dataset_id).to eq('my_dataset')
            expect(table_reference.table_id).to eq('my_table')
          end
        end

        it 'returns a Table' do
          table = dataset.create_table('my_table')
          expect(table).to be_a(Table)
        end

        it 'creates the table in the specified project and dataset' do
          dataset.create_table('my_table')
          expect(big_query_service).to have_received(:insert_table).with('my_project', 'my_dataset', anything)
        end

        context 'when a schema is specified' do
          it 'passes it along' do
            schema = {'fields' => [{'name' => 'id', 'type' => 'INTEGER', 'mode' => 'REQUIRED'}, {'name' => 'fax_number', 'type' => 'STRING', 'mode' => 'NULLABLE'}]}
            table_schema = nil
            allow(big_query_service).to receive(:insert_table) do |_, _, table|
              table_schema = table.schema
            end
            dataset.create_table('my_table', schema: schema)
            expect(table_schema.fields.map(&:name)).to eq(%w[id fax_number])
            expect(table_schema.fields.map(&:type)).to eq(%w[INTEGER STRING])
            expect(table_schema.fields.map(&:mode)).to eq(%w[REQUIRED NULLABLE])
          end
        end
      end
    end
  end
end
