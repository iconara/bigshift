module RS2BQ
  describe BigQuerySchemaBuilder do
    let :schema_builder do
      described_class.new(redshift_connection)
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

    describe '#big_query_schema_from' do
      it 'queries the "pg_table_def" table filtering by the specified table' do
        schema_builder.big_query_schema_from('some_table')
        expect(redshift_connection).to have_received(:exec_params).with(anything, ['some_table'])
      end

      it 'loads the column names, types and nullity' do
        schema_builder.big_query_schema_from('some_table')
        expect(redshift_connection).to have_received(:exec_params).with(/SELECT "column", "type", "notnull"/, anything)
      end

      context 'returns a BigQuery schema that' do
        let :big_query_schema do
          schema_builder.big_query_schema_from('some_table')
        end

        let :types do
          big_query_schema['fields'].each_with_object({}) { |f, m| m[f['name']] = f['type'] }
        end

        let :modes do
          big_query_schema['fields'].each_with_object({}) { |f, m| m[f['name']] = f['mode'] }
        end

        it 'contains all the columns from the Redshift table' do
          field_names = big_query_schema['fields'].map { |f| f['name'] }
          expect(field_names).to contain_exactly('id', 'name', 'fax_number', 'year_of_birth')
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

          it 'converts a Redshift TIMESTAMP column' do
            column_rows.first['type'] = 'timestamp without time zone'
            expect(types['id']).to eq('STRING')
          end

          it 'converts a Redshift DATE column' do
            column_rows.first['type'] = 'date'
            expect(types['id']).to eq('STRING')
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
          expect { schema_builder.big_query_schema_from('some_table') }.to raise_error('Unsupported column type: "imaginary"')
        end
      end

      context 'when the Redshift table does not exist' do
        before do
          allow(redshift_connection).to receive(:exec_params).with(anything, ['another_table']).and_return([])
        end

        it 'raises an error' do
          expect { schema_builder.big_query_schema_from('another_table') }.to raise_error('Table not found: "another_table"')
        end
      end
    end
  end
end
