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
        columns_by_name = columns.each_with_object({}) do |column, columns|
          columns[column.name] = column
        end
        aggregate_failures do
          expect(columns_by_name['id'].name).to eq('id')
          expect(columns_by_name['name'].name).to eq('name')
          expect(columns_by_name['fax_number'].type).to eq('character varying(100)')
          expect(columns_by_name['year_of_birth'].nullable?).to eq(true)
          expect(columns_by_name['id'].nullable?).to eq(false)
        end
      end

      it 'returns the columns in alphabetical order' do
        columns = table_schema.columns
        expect(columns.map(&:name)).to eq(%w[fax_number id name year_of_birth])
      end
    end
  end
end
