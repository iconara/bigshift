module BigShift
  describe RedshiftTableSchema do
    let :table_schema do
      described_class.new('some_schema', 'some_table', redshift_connection)
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
      allow(redshift_connection).to receive(:exec_params).with(/SELECT .+ FROM "pg_table_def" WHERE "schemaname" = \$1 AND "tablename" = \$2/, anything).and_return(column_rows)
    end

    describe '#columns' do
      it 'queries the "pg_table_def" table filtering by the specified table' do
        table_schema.columns
        expect(redshift_connection).to have_received(:exec_params).with(anything, ['some_schema', 'some_table'])
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

    describe '#to_big_query' do
      context 'returns a TableSchema that' do
        let :big_query_table_schema do
          table_schema.to_big_query
        end

        let :modes do
          big_query_table_schema.fields.each_with_object({}) do |field, acc|
            acc[field.name] = field.mode
          end
        end

        let :types do
          big_query_table_schema.fields.each_with_object({}) do |field, acc|
            acc[field.name] = field.type
          end
        end

        it 'contains all Redshift columns as fields, in alphabetical order' do
          expect(big_query_table_schema.fields.map(&:name)).to eq(%w[fax_number id name year_of_birth])
        end

        it 'sets the mode to REQUIRED where NOT NULL and NULLABLE where NULL in Redshift' do
          expect(modes).to eq(
            'id' => 'REQUIRED',
            'name' => 'REQUIRED',
            'fax_number' => 'NULLABLE',
            'year_of_birth' => 'NULLABLE'
          )
        end

        context 'uses the BigQuery type STRING when' do
          it 'converts a Redshift CHAR column' do
            column_rows.first['type'] = 'character(10)'
            expect(types['id']).to eq('STRING')
          end

          it 'converts a Redshift VARCHAR column' do
            column_rows.first['type'] = 'character varying(1000)'
            expect(types['id']).to eq('STRING')
          end

          it 'converts a Redshift DECIMAL column' do
            column_rows.first['type'] = 'numeric(18,0)'
            expect(types['id']).to eq('STRING')
          end

          it 'converts a Redshift DATE column' do
            column_rows.first['type'] = 'date'
            expect(types['id']).to eq('STRING')
          end
        end

        context 'uses the BigQuery type TIMESTAMP when' do
          it 'converts a Redshift TIMESTAMP column' do
            column_rows.first['type'] = 'timestamp without time zone'
            expect(types['id']).to eq('TIMESTAMP')
          end
        end

        context 'uses the BigQuery type INTEGER when' do
          it 'converts a Redshift SMALLINT column' do
            column_rows.first['type'] = 'smallint'
            expect(types['id']).to eq('INTEGER')
          end

          it 'converts a Redshift INTEGER column' do
            column_rows.first['type'] = 'integer'
            expect(types['id']).to eq('INTEGER')
          end

          it 'converts a Redshift BIGINT column' do
            column_rows.first['type'] = 'bigint'
            expect(types['id']).to eq('INTEGER')
          end
        end

        context 'uses the BigQuery type FLOAT when' do
          it 'converts a Redshift REAL column' do
            column_rows.first['type'] = 'real'
            expect(types['id']).to eq('FLOAT')
          end

          it 'converts a Redshift DOUBLE PRECISION column' do
            column_rows.first['type'] = 'double precision'
            expect(types['id']).to eq('FLOAT')
          end
        end

        context 'uses the BigQuery type BOOLEAN when' do
          it 'converts a Redshift BOOLEAN column' do
            column_rows.first['type'] = 'boolean'
            expect(types['id']).to eq('BOOLEAN')
          end
        end
      end

      context 'when a Redshift column has an unsupported type' do
        it 'raises an error' do
          column_rows.first['type'] = 'imaginary'
          expect { table_schema.to_big_query }.to raise_error('Unsupported column type: "imaginary"')
        end
      end

      context 'when the Redshift table does not exist' do
        before do
          allow(redshift_connection).to receive(:exec_params).with(anything, ['another_schema', 'another_table']).and_return([])
        end

        it 'raises an error' do
          expect { described_class.new('another_schema', 'another_table', redshift_connection).to_big_query }.to raise_error('Table "another_table" for schema "another_schema" not found')
        end
      end
    end
  end

  describe RedshiftTableSchema::Column do
    describe '#to_sql' do
      let :column do
        column = described_class.new(name, type, nullable)
      end

      let :type do
        'int'
      end

      let :name do
        'the_column'
      end

      let :nullable do
        false
      end

      it 'quotes the column name' do
        expect(column.to_sql).to eq('"the_column"')
      end

      context 'when the column type is CHAR or VARCHAR' do
        let :type do
          'character varying(100)'
        end

        it 'quotes the field in double quotes' do
          aggregate_failures do
            expect(column.to_sql).to start_with(%q<('"' ||>)
            expect(column.to_sql).to end_with(%q<|| '"')>)
          end
        end

        it 'replaces double quotes with two double quotes, as per the CSV spec' do
          expect(column.to_sql).to match(/REPLACE(.+, '"', '""')/)
        end

        it 'escapes newlines and carriage returns' do
          aggregate_failures do
            expect(column.to_sql).to match(/REPLACE(.+, '\\n', '\\\\n')/)
            expect(column.to_sql).to match(/REPLACE(.+, '\\r', '\\\\r')/)
          end
        end
      end

      context 'when the column type is BOOLEAN' do
        let :type do
          'boolean'
        end

        it 'returns SQL that converts the value to 1 or 0' do
          expect(column.to_sql).to eq('(CASE WHEN "the_column" THEN 1 ELSE 0 END)')
        end

        context 'and the column is nullable' do
          let :nullable do
            true
          end

          it 'returns SQL that converts the value to 1, 0 or NULL' do
            expect(column.to_sql).to eq('(CASE WHEN "the_column" IS NULL THEN NULL WHEN "the_column" THEN 1 ELSE 0 END)')
          end
        end
      end

      context 'when the column type is TIMESTAMP' do
        let :type do
          'timestamp without time zone'
        end

        it 'returns SQL that converts the timestamp to a UNIX timestamp with fractional seconds' do
          expect(column.to_sql).to eq('(EXTRACT(epoch FROM "the_column") + EXTRACT(milliseconds FROM "the_column")/1000.0)')
        end
      end

      context 'when the column type is DATE' do
        let :type do
          'date'
        end

        it 'returns SQL that converts the timestamp to a ISO 8601 formatted string' do
          expect(column.to_sql).to eq('(TO_CHAR("the_column", \'YYYY-MM-DD\'))')
        end
      end
    end
  end
end
