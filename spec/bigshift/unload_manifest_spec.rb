module BigShift
  describe UnloadManifest do
    let :manifest do
      described_class.new(s3_resource, 'my-bucket', 'some/prefix/')
    end

    let :s3_resource do
      double(:s3_resource)
    end

    let :manifest_object do
      double(:manifest_object)
    end

    let :raw_manifest do
      {
        'entries' => [
          {'url' => 's3://my-bucket/some/prefix/0000_part_00'},
          {'url' => 's3://my-bucket/some/prefix/0001_part_00'},
          {'url' => 's3://my-bucket/some/prefix/0002_part_00'},
        ]
      }
    end

    before do
      s3_bucket = double(:s3_bucket)
      manifest_result = double(:manifest_result)
      allow(s3_resource).to receive(:bucket).with('my-bucket').and_return(s3_bucket)
      allow(s3_bucket).to receive(:object).with('some/prefix/manifest').and_return(manifest_object)
      allow(manifest_object).to receive(:get).and_return(manifest_result)
      allow(manifest_result).to receive(:body).and_return(JSON.dump(raw_manifest))
    end

    describe '#bucket_name' do
      it 'returns the bucket name' do
        expect(manifest.bucket_name).to eq('my-bucket')
      end
    end

    describe '#prefix' do
      it 'returns the prefix' do
        expect(manifest.prefix).to eq('some/prefix/')
      end
    end

    describe '#manifest_key' do
      it 'returns the prefix + "manifest"' do
        expect(manifest.manifest_key).to eq('some/prefix/manifest')
      end
    end

    describe '#keys' do
      it 'downloads the manifest and returns the keys of its entries' do
        expect(manifest.keys).to eq([
          'some/prefix/0000_part_00',
          'some/prefix/0001_part_00',
          'some/prefix/0002_part_00',
        ])
      end

      it 'caches the results' do
        manifest.keys
        manifest.keys
        expect(manifest_object).to have_received(:get).once
      end
    end

    describe '#size' do
      it 'returns the number of keys' do
        expect(manifest.size).to eq(3)
      end
    end
  end
end
