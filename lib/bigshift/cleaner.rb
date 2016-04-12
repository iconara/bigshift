module BigShift
  class Cleaner
    def initialize(s3_resource, cs_service, options={})
      @s3_resource = s3_resource
      @cs_service = cs_service
      @logger = options[:logger] || NullLogger.new
    end

    def cleanup(unload_manifest, cs_bucket_name)
      cleanup_s3(unload_manifest)
      cleanup_cs(cs_bucket_name, unload_manifest)
      nil
    end

    private

    def cleanup_s3(unload_manifest)
      objects = unload_manifest.keys.map { |k| {key: k} }
      objects << {key: unload_manifest.manifest_key}
      @logger.info(sprintf('Deleting %d files from s3://%s/%s (including the manifest file)', objects.size, unload_manifest.bucket_name, unload_manifest.prefix))
      @s3_resource.bucket(unload_manifest.bucket_name).delete_objects(delete: {objects: objects})
    end

    def cleanup_cs(bucket_name, unload_manifest)
      @logger.info(sprintf('Deleting %d files from gs://%s/%s', unload_manifest.size, bucket_name, unload_manifest.prefix))
      unload_manifest.keys.each do |key|
        @cs_service.delete_object(bucket_name, key)
      end
    end
  end
end
