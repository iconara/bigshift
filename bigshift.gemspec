# encoding: utf-8

$: << File.expand_path('../lib', __FILE__)

require 'bigshift/version'

Gem::Specification.new do |s|
  s.name          = 'bigshift'
  s.version       = BigShift::VERSION.dup
  s.license       = 'BSD-3-Clause'
  s.authors       = ['Theo Hultberg']
  s.email         = ['theo@iconara.net']
  s.homepage      = 'http://github.com/iconara/bigshift'
  s.summary       = %q{A tool for moving tables from Redshift to BigQuery}
  s.description   = %q{BigShift is a tool for moving tables from Redshift
                       to BigQuery. It will create a table in BigQuery with
                       a schema that matches the Redshift table, dump the
                       data to S3, transfer it to GCS and finally load it
                       into the BigQuery table.}

  s.files         = Dir['bin/bigshift', 'lib/**/*.rb', 'README.md', 'LICENSE.txt', '.yardopts']
  s.require_paths = %w(lib)
  s.bindir        = 'bin'
  s.executables   = %w[bigshift]

  s.platform = Gem::Platform::RUBY
  s.required_ruby_version = '>= 1.9.3'

  s.add_runtime_dependency 'pg'
  s.add_runtime_dependency 'google-api-client', '~> 0.9'
  s.add_runtime_dependency 'googleauth'
  s.add_runtime_dependency 'google-cloud-env'
  s.add_runtime_dependency 'aws-sdk'
end
