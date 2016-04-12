module BigShift
  class UnloadManifest
    attr_reader :bucket_name, :prefix, :manifest_key

    def initialize(s3_resource, bucket_name, prefix)
      @s3_resource = s3_resource
      @bucket_name = bucket_name
      @prefix = prefix
      @manifest_key = "#{@prefix}manifest"
    end

    def keys
      @keys ||= begin
        bucket = @s3_resource.bucket(@bucket_name)
        object = bucket.object(@manifest_key)
        manifest = JSON.load(object.get.body)
        manifest['entries'].map { |entry| entry['url'].sub(%r{\As3://[^/]+/}, '') }
      end
    end

    def size
      keys.size
    end
  end
end
