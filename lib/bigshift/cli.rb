require 'pg'
require 'yaml'
require 'json'
require 'stringio'
require 'logger'
require 'optparse'
require 'bigshift'

module BigShift
  class CliError < BigShiftError
    attr_reader :details, :usage

    def initialize(message, details, usage)
      super(message)
      @details = details
      @usage = usage
    end
  end

  class Cli
    def initialize(argv, options={})
      @argv = argv.dup
      @factory_factory = options[:factory_factory] || Factory.method(:new)
    end

    def run
      setup
      unload
      transfer
      load
      cleanup
      nil
    end

    private

    def run?(step)
      @config[:steps].include?(step)
    end

    def setup
      @config = parse_args(@argv)
      @factory = @factory_factory.call(@config)
      @logger = @factory.logger
    end

    def unload
      if run?(:unload)
        s3_uri = "s3://#{@config[:s3_bucket_name]}/#{s3_table_prefix}"
        @factory.redshift_unloader.unload_to(@config[:rs_schema_name], @config[:rs_table_name], s3_uri, allow_overwrite: false, compression: @config[:compression])
      else
        @logger.debug('Skipping unload')
      end
      @unload_manifest = @factory.create_unload_manifest(@config[:s3_bucket_name], s3_table_prefix)
    end

    def transfer
      if run?(:transfer)
        description = "bigshift-#{@config[:rs_database_name]}-#{@config[:rs_schema_name]}-#{@config[:rs_table_name]}-#{Time.now.utc.strftime('%Y%m%dT%H%M')}"
        @factory.cloud_storage_transfer.copy_to_cloud_storage(@unload_manifest, @config[:cs_bucket_name], description: description, allow_overwrite: false)
      else
        @logger.debug('Skipping transfer')
      end
    end

    def load
      if run?(:load)
        rs_table_schema = @factory.redshift_table_schema
        bq_dataset = @factory.big_query_dataset
        bq_table = bq_dataset.table(@config[:bq_table_id]) || bq_dataset.create_table(@config[:bq_table_id])
        gcs_uri = "gs://#{@config[:cs_bucket_name]}/#{s3_table_prefix}*"
        options = {}
        options[:schema] = rs_table_schema.to_big_query
        options[:allow_overwrite] = true
        options[:max_bad_records] = @config[:max_bad_records] if @config[:max_bad_records]
        bq_table.load(gcs_uri, options)
      else
        @logger.debug('Skipping load')
      end
    end

    def cleanup
      if run?(:cleanup)
        @factory.cleaner.cleanup(@unload_manifest, @config[:cs_bucket_name])
      else
        @logger.debug('Skipping cleanup')
      end
    end

    STEPS = [
      :unload,
      :transfer,
      :load,
      :cleanup
    ].freeze

    ARGUMENTS = [
      ['--gcp-credentials', 'PATH', String, :gcp_credentials_path, :required],
      ['--aws-credentials', 'PATH', String, :aws_credentials_path, nil],
      ['--rs-credentials', 'PATH', String, :rs_credentials_path, :required],
      ['--rs-database', 'DB_NAME', String, :rs_database_name, :required],
      ['--rs-schema', 'SCHEMA_NAME', String, :rs_schema_name, nil],
      ['--rs-table', 'TABLE_NAME', String, :rs_table_name, :required],
      ['--bq-dataset', 'DATASET_ID', String, :bq_dataset_id, :required],
      ['--bq-table', 'TABLE_ID', String, :bq_table_id, nil],
      ['--s3-bucket', 'BUCKET_NAME', String, :s3_bucket_name, :required],
      ['--s3-prefix', 'PREFIX', String, :s3_prefix, nil],
      ['--cs-bucket', 'BUCKET_NAME', String, :cs_bucket_name, :required],
      ['--max-bad-records', 'N', Integer, :max_bad_records, nil],
      ['--steps', 'STEPS', Array, :steps, nil],
      ['--[no-]compression', nil, nil, :compression, nil],
    ]

    def parse_args(argv)
      config = {}
      parser = OptionParser.new do |p|
        ARGUMENTS.each do |flag, value_name, type, config_key, _|
          p.on("#{flag} #{value_name}", type) { |v| config[config_key] = v }
        end
      end
      config_errors = []
      begin
        parser.parse!(argv)
      rescue OptionParser::InvalidOption => e
        config_errors << e.message
      end
      %w[gcp aws rs].each do |prefix|
        if (path = config["#{prefix}_credentials_path".to_sym]) && File.exist?(path)
          config["#{prefix}_credentials".to_sym] = YAML.load(File.read(path))
        elsif path && !File.exist?(path)
          config_errors << sprintf('%s does not exist', path.inspect)
        end
      end
      ARGUMENTS.each do |flag, _, _, config_key, required|
        if !config.include?(config_key) && required
          config_errors << "#{flag} is required"
        end
      end
      config[:bq_table_id] ||= config[:rs_table_name]
      config[:rs_schema_name] ||= 'public'
      if config[:steps] && !config[:steps].empty?
        config[:steps] = STEPS.select { |s| config[:steps].include?(s.to_s) }
      else
        config[:steps] = STEPS
      end
      unless config_errors.empty?
        raise CliError.new('Configuration missing or malformed', config_errors, parser.to_s)
      end
      config
    end

    def s3_table_prefix
      @s3_table_prefix ||= begin
        db_name = @config[:rs_database_name]
        schema_name = @config[:rs_schema_name]
        table_name = @config[:rs_table_name]
        prefix = "#{db_name}/#{schema_name}/#{table_name}/#{db_name}-#{schema_name}-#{table_name}-"
        if (s3_prefix = @config[:s3_prefix])
          s3_prefix = s3_prefix.gsub(%r{\A/|/\Z}, '')
          prefix = "#{s3_prefix}/#{prefix}"
        end
        prefix
      end
    end
  end

  class Factory
    def initialize(config)
      @config = config
    end

    def redshift_unloader
      @redshift_unloader ||= RedshiftUnloader.new(rs_connection, aws_credentials, logger: logger)
    end

    def cloud_storage_transfer
      @cloud_storage_transfer ||= CloudStorageTransfer.new(cs_transfer_service, raw_gcp_credentials['project_id'], aws_credentials, logger: logger)
    end

    def redshift_table_schema
      @redshift_table_schema ||= RedshiftTableSchema.new(@config[:rs_schema_name], @config[:rs_table_name], rs_connection)
    end

    def big_query_dataset
      @big_query_dataset ||= BigQuery::Dataset.new(bq_service, raw_gcp_credentials['project_id'], @config[:bq_dataset_id], logger: logger)
    end

    def cleaner
      @cleaner ||= Cleaner.new(s3_resource, cs_service, logger: logger)
    end

    def s3_resource
      @s3_resource ||= Aws::S3::Resource.new(
        region: aws_region,
        credentials: aws_credentials
      )
    end

    def logger
      @logger ||= Logger.new($stderr)
    end

    def create_unload_manifest(s3_bucket_name, s3_table_prefix)
      UnloadManifest.new(s3_resource, cs_service, @config[:s3_bucket_name], s3_table_prefix)
    end

    private

    def rs_connection
      @rs_connection ||= PG.connect(
        host: @config[:rs_credentials]['host'],
        port: @config[:rs_credentials]['port'],
        dbname: @config[:rs_database_name],
        user: @config[:rs_credentials]['username'],
        password: @config[:rs_credentials]['password'],
        sslmode: 'require'
      )
      @rs_connection.exec("SET search_path = #{@config[:rs_schema_name]}")
      @rs_connection
    end

    def cs_transfer_service
      @cs_transfer_service ||= begin
        s = Google::Apis::StoragetransferV1::StoragetransferService.new
        s.authorization = gcp_credentials
        s
      end
    end

    def cs_service
      @cs_service ||= begin
        s = Google::Apis::StorageV1::StorageService.new
        s.authorization = gcp_credentials
        s
      end
    end

    def bq_service
      @bq_service ||= begin
        s = Google::Apis::BigqueryV2::BigqueryService.new
        s.authorization = gcp_credentials
        s
      end
    end

    def aws_credentials
      @aws_credentials ||= begin
        if @config[:aws_credentials]
          credentials = Aws::Credentials.new(*@config[:aws_credentials].values_at('access_key_id', 'secret_access_key'))
        else
          credentials = nil
        end
        if (credentials = Aws::CredentialProviderChain.new(credentials).resolve)
          credentials
        else
          raise 'No AWS credentials found'
        end
      end
    end

    def aws_region
      @aws_region ||= ((awsc = @config[:aws_credentials]) && awsc['region']) || ENV['AWS_REGION'] || ENV['AWS_DEFAULT_REGION']
    end

    def raw_gcp_credentials
      @config[:gcp_credentials]
    end

    def gcp_credentials
      @gcp_credentials ||= Google::Auth::ServiceAccountCredentials.make_creds(
        json_key_io: StringIO.new(JSON.dump(raw_gcp_credentials)),
        scope: Google::Apis::StoragetransferV1::AUTH_CLOUD_PLATFORM
      )
    end
  end
end
