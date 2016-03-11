module RS2BQ
  describe BigQueryTableCreator do
    let :table_creator do
      described_class.new(big_query_dataset, schema_builder)
    end

    let :big_query_dataset do
      double(:big_query_dataset)
    end

    let :schema_builder do
      double(:schema_builder)
    end

    let :generated_schema do
      {'fields' => [{'name' => 'my_column', 'type' => 'STRING', 'mode' => 'REQUIRED'}]}
    end

    before do
      allow(big_query_dataset).to receive(:create_table)
      allow(schema_builder).to receive(:big_query_schema_from).with('my_redshift_table').and_return(generated_schema)
    end

    describe '#create_table_like' do
      it 'creates a BigQuery table' do
        table_creator.create_table_like('my_bq_table', 'my_redshift_table')
        expect(big_query_dataset).to have_received(:create_table).with('my_bq_table', anything)
      end

      it 'uses the schema builder to create a schema from the Redshift table' do
        table_creator.create_table_like('my_bq_table', 'my_redshift_table')
        expect(big_query_dataset).to have_received(:create_table).with(anything, hash_including(schema: generated_schema))
      end
    end
  end
end
