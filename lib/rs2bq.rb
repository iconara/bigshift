require 'google/apis/bigquery_v2'
require 'google/apis/storagetransfer_v1'

module RS2BQ
  class NullLogger
    def close(*); end
    def debug(*); end
    def debug?; false end
    def error(*); end
    def error?; false end
    def fatal(*); end
    def fatal?; false end
    def info(*); end
    def info?; false end
    def unknown(*); end
    def warn(*); end
    def warn?; false end

    INSTANCE = new
  end
end

require 'rs2bq/big_query_dataset'
require 'rs2bq/big_query_schema_builder'
require 'rs2bq/big_query_table_creator'
require 'rs2bq/redshift_table_schema'
require 'rs2bq/redshift_unloader'
require 'rs2bq/cloud_storage_transfer'
