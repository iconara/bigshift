module BigShift
  describe Cli do
    let :cli do
      described_class.new(argv, factory_factory: factory_factory)
    end

    let :factory_factory do
      double(:factory_factory)
    end

    let :factory do
      double(:factory)
    end

    let :redshift_unloader do
      double(:redshift_unloader)
    end

    let :redshift_table_schema do
      double(:redshift_table_schema)
    end

    let :cloud_storage_transfer do
      double(:cloud_storage_transfer)
    end

    let :big_query_dataset do
      double(:big_query_dataset)
    end

    let :big_query_table do
      double(:big_query_table)
    end

    let :big_query_table_schema do
      double(:big_query_table_schema)
    end

    let :gcp_credentials do
      {
        'type' => 'service_account',
        'project_id' => 'polished-carrot-1234567',
        'private_key_id' => '67ef3daf89debadc47f00d098fe8db42',
      }.freeze
    end

    let :aws_credentials do
      {
        'aws_access_key_id' => 'AKXYZABC123FOOBARBAZ',
        'aws_secret_access_key' => 'eW91ZmlndXJlZG91dGl0d2FzYmFzZTY0ISEhCg',
      }.freeze
    end

    let :rs_credentials do
      {
        'host' => 'my-cluster.abc123.eu-west-1.redshift.amazonaws.com',
        'port' => 5439,
        'username' => 'my_redshift_user',
        'password' => 'dGhpc2lzYWxzb2Jhc2U2NAo',
      }.freeze
    end

    let :argv do
      [
        '--s3-bucket', 'the-s3-staging-bucket',
        '--rs-database', 'the_rs_database',
        '--rs-table', 'the_rs_table',
        '--cs-bucket', 'the-cs-bucket',
        '--bq-dataset', 'the_bq_dataset',
        '--bq-table', 'the_bq_table',
        '--gcp-credentials', 'gcp-credentials.yml',
        '--aws-credentials', 'aws-credentials.yml',
        '--rs-credentials', 'rs-credentials.yml',
      ]
    end

    def write_config
      File.write('gcp-credentials.yml', JSON.dump(gcp_credentials))
      File.write('aws-credentials.yml', JSON.dump(aws_credentials))
      File.write('rs-credentials.yml', JSON.dump(rs_credentials))
    end

    around do |example|
      Dir.mktmpdir do |path|
        Dir.chdir(path) do
          example.call
        end
      end
    end

    before do
      write_config
    end

    before do
      allow(factory_factory).to receive(:call).and_return(factory)
      allow(factory).to receive(:redshift_unloader).and_return(redshift_unloader)
      allow(factory).to receive(:cloud_storage_transfer).and_return(cloud_storage_transfer)
      allow(factory).to receive(:big_query_dataset).and_return(big_query_dataset)
      allow(factory).to receive(:redshift_table_schema).and_return(redshift_table_schema)
      allow(redshift_unloader).to receive(:unload_to)
      allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage)
      allow(big_query_dataset).to receive(:table).with('the_bq_table').and_return(big_query_table)
      allow(big_query_table).to receive(:load)
      allow(redshift_table_schema).to receive(:to_big_query).and_return(big_query_table_schema)
    end

    describe '#run' do
      it 'unloads the Redshift table to S3' do
        cli.run
        expect(redshift_unloader).to have_received(:unload_to).with('the_rs_table', 's3://the-s3-staging-bucket/the_rs_database/the_rs_table/', anything)
      end

      it 'transfers the unloaded data to Cloud Storage' do
        cli.run
        expect(cloud_storage_transfer).to have_received(:copy_to_cloud_storage).with('the-s3-staging-bucket', 'the_rs_database/the_rs_table/', 'the-cs-bucket', anything)
      end

      it 'gives the transfer a description that contains the Redshift database and table names, and the current time' do
        description = nil
        allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage) do |_, _, _, options|
          description = options[:description]
        end
        cli.run
        expect(description).to match(/\Abigshift-the_rs_database-the_rs_table-\d{8}T\d{4}\Z/)
      end

      it 'loads the transferred data' do
        cli.run
        expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/the_rs_database/the_rs_table/*', anything)
      end

      it 'converts the Redshift table\'s schema and uses it when loading the BigQuery table' do
        cli.run
        expect(big_query_table).to have_received(:load).with(anything, hash_including(schema: big_query_table_schema))
      end

      it 'deletes the unloaded data on S3'

      it 'deletes the transferred data on Cloud Storage'

      it 'creates the necessary components using the specified config parameters' do
        cli.run
        expect(factory_factory).to have_received(:call).with(hash_including(
          :s3_bucket_name => 'the-s3-staging-bucket',
          :rs_database_name => 'the_rs_database',
          :rs_table_name => 'the_rs_table',
          :cs_bucket_name => 'the-cs-bucket',
          :bq_dataset_id => 'the_bq_dataset',
          :bq_table_id => 'the_bq_table',
        ))
      end

      it 'reads the specified AWS configuration' do
        cli.run
        expect(factory_factory).to have_received(:call).with(hash_including(
          :gcp_credentials => gcp_credentials,
          :aws_credentials => aws_credentials,
          :rs_credentials => rs_credentials,
        ))
      end

      context 'with an S3 prefix' do
        let :argv do
          super() + ['--s3-prefix', 'and/the/prefix']
        end

        it 'includes the prefix in the config' do
          cli.run
          expect(factory_factory).to have_received(:call).with(hash_including(
            :s3_prefix => 'and/the/prefix',
          ))
        end

        it 'unloads to a location on S3 under the specified prefix' do
          cli.run
          expect(redshift_unloader).to have_received(:unload_to).with(anything, 's3://the-s3-staging-bucket/and/the/prefix/the_rs_database/the_rs_table/', anything)
        end

        it 'transfers that S3 location' do
          cli.run
          expect(cloud_storage_transfer).to have_received(:copy_to_cloud_storage).with(anything, 'and/the/prefix/the_rs_database/the_rs_table/', anything, anything)
        end

        it 'loads from that location' do
          cli.run
          expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/and/the/prefix/the_rs_database/the_rs_table/*', anything)
        end
      end

      context 'with the --max-bad-records arguments' do
        let :argv do
          super() + ['--max-bad-records', '3']
        end

        it 'specifies the :max_bad_records option when loading the BigQuery table' do
          cli.run
          expect(big_query_table).to have_received(:load).with(anything, hash_including(max_bad_records: 3))
        end
      end

      context 'when the BigQuery table does not exist' do
        before do
          allow(big_query_dataset).to receive(:table).and_return(nil)
          allow(big_query_dataset).to receive(:create_table).and_return(big_query_table)
        end

        it 'creates the table' do
          cli.run
          expect(big_query_dataset).to have_received(:create_table).with('the_bq_table')
        end
      end

      %w[gcp aws rs].each do |prefix|
        context "when --#{prefix}-credentials is not specified" do
          let :argv do
            a = super()
            i = a.index("--#{prefix}-credentials")
            a.delete_at(i)
            a.delete_at(i)
            a
          end

          it 'raises an error' do
            error = nil
            begin
              cli.run
            rescue => e
              error = e
            end
            expect(error.details).to include("--#{prefix}-credentials is required")
          end
        end

        context "when the path given to --#{prefix}-credentials does not exist" do
          before do
            FileUtils.rm_f("#{prefix}-credentials.yml")
          end

          it 'raises an error' do
            error = nil
            begin
              cli.run
            rescue => e
              error = e
            end
            expect(error.details).to include(%<"#{prefix}-credentials.yml" does not exist>)
          end
        end
      end

      %w[
        --rs-database
        --rs-table
        --bq-dataset
        --bq-table
        --s3-bucket
        --cs-bucket
      ].each do |flag|
        context "when #{flag} is not specified" do
          let :argv do
            a = super()
            i = a.index(flag)
            a.delete_at(i)
            a.delete_at(i)
            a
          end

          it 'raises an error' do
            error = nil
            begin
              cli.run
            rescue => e
              error = e
            end
            expect(error.details).to include("#{flag} is required")
          end
        end
      end

      context 'when an unknown argument is specified' do
        let :argv do
          super() + ['--foo', 'bar']
        end

        it 'raises an error' do
          error = nil
          begin
            cli.run
          rescue => e
            error = e
          end
          expect(error.details).to include("invalid option: --foo")
        end
      end
    end
  end
end
