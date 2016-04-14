module BigShift
  describe UnloadManifest do
    let :manifest do
      described_class.new(s3_resource, 'my-bucket', 'some/prefix/')
    end

    let :s3_resource do
      double(:s3_resource)
    end

    let :s3_bucket do
      double(:s3_bucket)
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

    describe '#count' do
      it 'returns the number of keys' do
        expect(manifest.count).to eq(3)
      end
    end

    describe '#total_file_size' do
      let :objects do
        [
          double(:object1, key: 'some/prefix/0000_part_00', size: 3),
          double(:object2, key: 'some/prefix/0001_part_00', size: 5),
          double(:object3, key: 'some/prefix/0002_part_00', size: 7),
        ]
      end

      before do
        allow(s3_bucket).to receive(:objects).with(prefix: 'some/prefix/').and_return(objects)
      end

      it 'lists the files and sums up their total size, in bytes' do
        expect(manifest.total_file_size).to eq(3 + 5 + 7)
      end

      it 'caches the results' do
        manifest.total_file_size
        manifest.total_file_size
        expect(s3_bucket).to have_received(:objects).once
      end

      context 'when the listing contains files not in the manifest' do
        let :objects do
          [
            double(:object1, key: 'some/prefix/0000_part_00', size: 3),
            double(:object2, key: 'some/prefix/0001_part_00', size: 5),
            double(:object3, key: 'some/prefix/0002_part_00', size: 7),
            double(:object4, key: 'some/prefix/manifest', size: 1),
            double(:object5, key: 'some/prefix/random_file', size: 11),
          ]
        end

        it 'lists the files and sums up their total size, in bytes' do
          expect(manifest.total_file_size).to eq(3 + 5 + 7)
        end
      end
    end
  end
end
