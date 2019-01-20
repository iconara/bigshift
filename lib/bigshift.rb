require 'google/apis/bigquery_v2'
require 'google/apis/storagetransfer_v1'
require 'google/apis/storage_v1'
require 'google/cloud/env'
require 'aws-sdk-s3'

module BigShift
  BigShiftError = Class.new(StandardError)

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

require 'bigshift/big_query/dataset'
require 'bigshift/big_query/table'
require 'bigshift/redshift_table_schema'
require 'bigshift/redshift_unloader'
require 'bigshift/cloud_storage_transfer'
require 'bigshift/unload_manifest'
require 'bigshift/cleaner'
