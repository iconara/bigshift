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

    let :cleaner do
      double(:cleaner)
    end

    let :big_query_table_schema do
      double(:big_query_table_schema)
    end

    let :logger do
      double(:logger, debug: nil)
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
        '--gcp-credentials', 'gcp-credentials.yml',
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
      allow(factory).to receive(:cleaner).and_return(cleaner)
      allow(factory).to receive(:s3_resource).and_return(nil)
      allow(factory).to receive(:logger).and_return(logger)
      allow(redshift_unloader).to receive(:unload_to)
      allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage)
      allow(big_query_dataset).to receive(:table).with('the_rs_table').and_return(big_query_table)
      allow(big_query_table).to receive(:load)
      allow(redshift_table_schema).to receive(:to_big_query).and_return(big_query_table_schema)
      allow(cleaner).to receive(:cleanup)
    end

    describe '#run' do
      it 'unloads the Redshift table to S3' do
        cli.run
        expect(redshift_unloader).to have_received(:unload_to).with('the_rs_table', 's3://the-s3-staging-bucket/the_rs_database/the_rs_table/', anything)
      end

      it 'does not allow the S3 location to be overwritten' do
        cli.run
        expect(redshift_unloader).to have_received(:unload_to).with(anything, anything, hash_including(allow_overwrite: false))
      end

      it 'transfers the unloaded data to Cloud Storage' do
        unload_manifest = nil
        allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage) do |um, _, _|
          unload_manifest = um
        end
        cli.run
        aggregate_failures do
          expect(cloud_storage_transfer).to have_received(:copy_to_cloud_storage).with(anything, 'the-cs-bucket', anything)
          expect(unload_manifest.bucket_name).to eq('the-s3-staging-bucket')
          expect(unload_manifest.prefix).to eq('the_rs_database/the_rs_table/')
        end
      end

      it 'gives the transfer a description that contains the Redshift database and table names, and the current time' do
        description = nil
        allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage) do |_, _, options|
          description = options[:description]
        end
        cli.run
        expect(description).to match(/\Abigshift-the_rs_database-the_rs_table-\d{8}T\d{4}\Z/)
      end

      it 'does not allow the Cloud Storage destination to be overwritten' do
        cli.run
        expect(cloud_storage_transfer).to have_received(:copy_to_cloud_storage).with(anything, anything, hash_including(allow_overwrite: false))
      end

      it 'loads the transferred data' do
        cli.run
        expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/the_rs_database/the_rs_table/*', anything)
      end

      it 'loads the transferred data into a table with the same name as the Redshift table' do
        cli.run
        expect(big_query_dataset).to have_received(:table).with('the_rs_table')
      end

      it 'converts the Redshift table\'s schema and uses it when loading the BigQuery table' do
        cli.run
        expect(big_query_table).to have_received(:load).with(anything, hash_including(schema: big_query_table_schema))
      end

      it 'deletes the unloaded data on S3 and Cloud Storage' do
        unload_manifest = nil
        cs_bucket_name = nil
        allow(cleaner).to receive(:cleanup) do |um, csbn|
          unload_manifest = um
          cs_bucket_name = csbn
        end
        cli.run
        expect(unload_manifest.bucket_name).to eq('the-s3-staging-bucket')
        expect(cs_bucket_name).to eq('the-cs-bucket')
      end

      it 'creates the necessary components using the specified config parameters' do
        cli.run
        expect(factory_factory).to have_received(:call).with(hash_including(
          :s3_bucket_name => 'the-s3-staging-bucket',
          :rs_database_name => 'the_rs_database',
          :rs_table_name => 'the_rs_table',
          :cs_bucket_name => 'the-cs-bucket',
          :bq_dataset_id => 'the_bq_dataset',
        ))
      end

      it 'reads the specified configuration files' do
        cli.run
        expect(factory_factory).to have_received(:call).with(hash_including(
          :gcp_credentials => gcp_credentials,
          :rs_credentials => rs_credentials,
        ))
      end

      it 'uses the AWS SDK\'s credentials resolution mechanisms'

      context 'with the --s3-prefix argument' do
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
          unload_manifest = nil
          allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage) do |um, _, _|
            unload_manifest = um
          end
          cli.run
          expect(unload_manifest.prefix).to eq('and/the/prefix/the_rs_database/the_rs_table/')
        end

        it 'loads from that location' do
          cli.run
          expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/and/the/prefix/the_rs_database/the_rs_table/*', anything)
        end

        context 'and it has a slash prefix or suffix' do
          it 'strips slashes from the front of the prefix' do
            argv[-1] = '/and/the/prefix'
            cli.run
            aggregate_failures do
              expect(redshift_unloader).to have_received(:unload_to).with(anything, 's3://the-s3-staging-bucket/and/the/prefix/the_rs_database/the_rs_table/', anything)
              expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/and/the/prefix/the_rs_database/the_rs_table/*', anything)
            end
          end

          it 'strips slashes from the end of the prefix' do
            argv[-1] = 'and/the/prefix/'
            cli.run
            aggregate_failures do
              expect(redshift_unloader).to have_received(:unload_to).with(anything, 's3://the-s3-staging-bucket/and/the/prefix/the_rs_database/the_rs_table/', anything)
              expect(big_query_table).to have_received(:load).with('gs://the-cs-bucket/and/the/prefix/the_rs_database/the_rs_table/*', anything)
            end
          end
        end
      end

      context 'with the --bq-table argument' do
        let :argv do
          super() + ['--bq-table', 'the_bq_table']
        end

        before do
          allow(big_query_dataset).to receive(:table).with('the_bq_table').and_return(big_query_table)
        end

        it 'loads into a BigQuery table by that name instead of a table with the same name as the Redshift table' do
          cli.run
          aggregate_failures do
            expect(factory_factory).to have_received(:call).with(hash_including(bq_table_id: 'the_bq_table'))
            expect(big_query_dataset).to have_received(:table).with('the_bq_table')
            expect(big_query_dataset).to_not have_received(:table).with('the_rs_table')
          end
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
          expect(big_query_dataset).to have_received(:create_table).with('the_rs_table')
        end
      end

      %w[gcp rs].each do |prefix|
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

      context 'when --aws-credentials is specified' do
        let :argv do
          super() + ['--aws-credentials', 'aws-credentials.yml']
        end

        it 'uses the credentials from the file'

        context 'and the file contains a region' do
          it 'uses the region from the file'
        end

        context 'but the file does not exist' do
          before do
            FileUtils.rm_f('aws-credentials.yml')
          end

          it 'raises an error' do
            error = nil
            begin
              cli.run
            rescue => e
              error = e
            end
            expect(error.details).to include('"aws-credentials.yml" does not exist')
          end
        end
      end

      context 'when --steps is specified' do
        let :argv do
          super() + ['--steps', 'load,cleanup']
        end

        it 'runs only the specified steps' do
          cli.run
          aggregate_failures do
            expect(redshift_unloader).to_not have_received(:unload_to)
            expect(cloud_storage_transfer).to_not have_received(:copy_to_cloud_storage)
            expect(big_query_table).to have_received(:load)
            expect(cleaner).to have_received(:cleanup)
          end
        end

        it 'logs when it skips a step' do
          cli.run
          aggregate_failures do
            expect(logger).to have_received(:debug).with('Skipping unload')
            expect(logger).to have_received(:debug).with('Skipping transfer')
          end
        end

        context 'and the unload step is not included' do
          let :argv do
            super() + ['--steps', 'transfer,load']
          end

          it 'still reads the unload manifest' do
            unload_manifest = nil
            allow(cloud_storage_transfer).to receive(:copy_to_cloud_storage) do |um, _, _|
              unload_manifest = um
            end
            cli.run
            expect(unload_manifest).to_not be_nil
          end
        end

        context 'and the steps are specified out of order' do
          let :argv do
            super() + ['--steps', 'transfer,cleanup,load']
          end

          it 'runs the steps in the correct order' do
            cli.run
            aggregate_failures do
              expect(cloud_storage_transfer).to have_received(:copy_to_cloud_storage).ordered
              expect(big_query_table).to have_received(:load).ordered
              expect(cleaner).to have_received(:cleanup).ordered
            end
          end
        end
      end

      %w[
        --rs-database
        --rs-table
        --bq-dataset
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
