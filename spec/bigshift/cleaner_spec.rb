module BigShift
  describe Cleaner do
    let :cleaner do
      described_class.new(s3_resource, cs_service, logger: logger)
    end

    let :s3_resource do
      double(:s3_resource)
    end

    let :s3_bucket do
      double(:s3_bucket)
    end

    let :cs_service do
      double(:cs_service)
    end

    let :logger do
      double(:logger, debug: nil, info: nil, warn: nil)
    end

    let :unload_manifest do
      double(:unload_manifest)
    end

    let :keys do
      %w[
        the/path/to/key1
        the/path/to/key2
        the/path/to/key3
      ]
    end

    before do
      allow(unload_manifest).to receive(:bucket_name).and_return('my-s3-bucket')
      allow(unload_manifest).to receive(:prefix).and_return('the/path/to/')
      allow(unload_manifest).to receive(:keys).and_return(keys)
      allow(unload_manifest).to receive(:size).and_return(keys.size)
      allow(unload_manifest).to receive(:manifest_key).and_return('the/path/to/manifest')
      allow(s3_resource).to receive(:bucket).with('my-s3-bucket').and_return(s3_bucket)
      allow(s3_bucket).to receive(:delete_objects)
      allow(cs_service).to receive(:delete_object)
    end

    describe '#cleanup' do
      it 'deletes the keys in the unload manifest from S3, and the manifest file' do
        delete_spec = nil
        allow(s3_bucket).to receive(:delete_objects) do |spec|
          delete_spec = spec
        end
        cleaner.cleanup(unload_manifest, 'my-cs-bucket')
        expect(delete_spec[:delete][:objects]).to eq([
          {:key => 'the/path/to/key1'},
          {:key => 'the/path/to/key2'},
          {:key => 'the/path/to/key3'},
          {:key => 'the/path/to/manifest'},
        ])
      end

      it 'logs that it deletes objects from S3' do
        cleaner.cleanup(unload_manifest, 'my-cs-bucket')
        expect(logger).to have_received(:info).with('Deleting 4 files from s3://my-s3-bucket/the/path/to/ (including the manifest file)')
      end

      it 'deletes each key in the unload manifest from Cloud Storage' do
        allow(cs_service).to receive(:delete_object)
        cleaner.cleanup(unload_manifest, 'my-cs-bucket')
        aggregate_failures do
          expect(cs_service).to have_received(:delete_object).with('my-cs-bucket', 'the/path/to/key1')
          expect(cs_service).to have_received(:delete_object).with('my-cs-bucket', 'the/path/to/key2')
          expect(cs_service).to have_received(:delete_object).with('my-cs-bucket', 'the/path/to/key3')
        end
      end

      it 'logs that it deletes objects from Cloud Storage' do
        cleaner.cleanup(unload_manifest, 'my-cs-bucket')
        expect(logger).to have_received(:info).with('Deleting 3 files from gs://my-cs-bucket/the/path/to/')
      end
    end
  end
end
