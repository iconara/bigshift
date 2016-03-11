module RS2BQ
  describe RedshiftTableSchema do
    let :table_schema do
      described_class.new('some_table', redshift_connection)
    end

    let :redshift_connection do
      double(:redshift_connection)
    end

    let :column_rows do
      [
        {'column' => 'id', 'type' => 'bigint', 'notnull' => 't'},
        {'column' => 'name', 'type' => 'character varying(100)', 'notnull' => 't'},
        {'column' => 'fax_number', 'type' => 'character varying(100)', 'notnull' => 'f'},
        {'column' => 'year_of_birth', 'type' => 'smallint', 'notnull' => 'f'},
      ]
    end

    before do
      allow(redshift_connection).to receive(:exec_params).with(/SELECT .+ FROM "pg_table_def" WHERE .+ "tablename" = \$1/, anything).and_return(column_rows)
    end

    describe '#columns' do
      it 'queries the "pg_table_def" table filtering by the specified table' do
        table_schema.columns
        expect(redshift_connection).to have_received(:exec_params).with(anything, ['some_table'])
      end

      it 'loads the column names, types and nullity' do
        table_schema.columns
        expect(redshift_connection).to have_received(:exec_params).with(/SELECT "column", "type", "notnull"/, anything)
      end

      it 'returns all columns as Column objects' do
        columns = table_schema.columns
        aggregate_failures do
          expect(columns[0].name).to eq('id')
          expect(columns[1].name).to eq('name')
          expect(columns[2].type).to eq('character varying(100)')
          expect(columns[3].nullable?).to eq(true)
          expect(columns[0].nullable?).to eq(false)
        end
      end
    end
  end
end
