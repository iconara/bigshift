module RS2BQ
  describe RedshiftUnloader do
    let :unloader do
      described_class.new(redshift_connection, aws_credentials, options)
    end

    let :redshift_connection do
      double(:redshift_connection)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil)
    end

    let :aws_credentials do
      {
        'aws_access_key_id' => 'foo',
        'aws_secret_access_key' => 'bar',
      }
    end

    let :options do
      {
        :logger => logger
      }
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

      it 'logs that it unloads' do
        expect(logger).to have_received(:info).with('Unloading Redshift table my_table to s3://my-bucket/here/')
      end

      context 'when the :allow_overwrite option is true' do
        let :options do
          super().merge(allow_overwrite: true)
        end

        it 'adds ALLOWOVERWRITE to the unload command' do
          expect(unload_command).to include(%q<ALLOWOVERWRITE>)
        end
      end

      context 'when Redshift datatypes need to be converted' do
        let :column_rows do
          super() << {'column' => 'alive', 'type' => 'boolean', 'notnull' => 't'}
        end

        it 'includes the necessary SQL in the unload command' do
          expect(unload_command).to include(%q<(CASE WHEN "alive" THEN 1 ELSE 0 END)>)
        end
      end
    end
  end
end
