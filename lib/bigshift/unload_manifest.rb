module BigShift
  TransferValidationError = Class.new(BigShiftError)

  class UnloadManifest
    attr_reader :bucket_name, :prefix, :manifest_key

    def initialize(s3_resource, cs_service, bucket_name, prefix)
      @s3_resource = s3_resource
      @cs_service = cs_service
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

    def count
      keys.size
    end

    def total_file_size
      @total_file_size ||= file_sizes.values.reduce(:+)
    end

    def validate_transfer(cs_bucket_name)
      objects = @cs_service.list_objects(cs_bucket_name, prefix: @prefix)
      cs_file_sizes = objects.items.each_with_object({}) do |item, acc|
        acc[item.name] = item.size.to_i
      end
      missing_files = (file_sizes.keys - cs_file_sizes.keys)
      extra_files = cs_file_sizes.keys - file_sizes.keys
      common_files = (cs_file_sizes.keys & file_sizes.keys)
      size_mismatches = common_files.select { |name| file_sizes[name] != cs_file_sizes[name] }
      errors = []
      unless missing_files.empty?
        errors << "missing files: #{missing_files.join(', ')}"
      end
      unless extra_files.empty?
        errors << "extra files: #{extra_files.join(', ')}"
      end
      unless size_mismatches.empty?
        messages = size_mismatches.map { |name| sprintf('%s (%d != %d)', name, cs_file_sizes[name], file_sizes[name]) }
        errors << "size mismatches: #{messages.join(', ')}"
      end
      unless errors.empty?
        raise TransferValidationError, "Transferred files don't match unload manifest: #{errors.join('; ')}"
      end
    end

    private

    def file_sizes
      @file_sizes ||= begin
        bucket = @s3_resource.bucket(@bucket_name)
        objects = bucket.objects(prefix: @prefix)
        objects.each_with_object({}) do |object, acc|
          if keys.include?(object.key)
            acc[object.key] = object.size
          end
        end
      end
    end
  end
end
