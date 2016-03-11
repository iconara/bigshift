module RS2BQ
  describe RedshiftUnloader do
    let :unloader do
      described_class.new(redshift_connection, aws_credentials, options)
    end

    let :redshift_connection do
      double(:redshift_connection)
    end

    let :aws_credentials do
      {
        'aws_access_key_id' => 'foo',
        'aws_secret_access_key' => 'bar',
      }
    end

    let :options do
      {}
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
      allow(redshift_connection).to receive(:exec)
      allow(redshift_connection).to receive(:exec_params).with(/^SELECT "column", .+ FROM "pg_table_def"/, ['my_table']).and_return(column_rows)
    end

    describe '#unload' do
      let :unload_command do
        ''
      end

      before do
        allow(redshift_connection).to receive(:exec) do |sql|
          unload_command.replace(sql)
        end
        unloader.unload_to('my_table', 's3://my-bucket/here/')
      end

      it 'executes an UNLOAD command' do
        expect(unload_command).to match(/^UNLOAD/)
      end

      it 'unloads to the specified S3 prefix' do
        expect(unload_command).to include(%q<TO 's3://my-bucket/here/'>)
      end

      it 'unloads CSV' do
        aggregate_failures do
          expect(unload_command).to include(%q<DELIMITER ','>)
          expect(unload_command).to include(%q<ADDQUOTES>)
          expect(unload_command).to include(%q<ESCAPE>)
        end
      end

      it 'adds the provided credentials to the unload command' do
        expect(unload_command).to include(%q<CREDENTIALS 'aws_access_key_id=foo;aws_secret_access_key=bar'>)
      end

      it 'explicitly selects all columns from the table' do
        expect(unload_command).to include(%q<('SELECT "fax_number", "id", "name", "year_of_birth" FROM "my_table"')>)
      end

      it 'does not allow overwrites of the destination' do
        expect(unload_command).not_to include(%q<ALLOWOVERWRITE>)
      end

      context 'when the :allow_overwrite option is true' do
        let :options do
          super().merge(allow_overwrite: true)
        end

        it 'adds ALLOWOVERWRITE to the unload command' do
          expect(unload_command).to include(%q<ALLOWOVERWRITE>)
        end
      end
    end
  end
end
