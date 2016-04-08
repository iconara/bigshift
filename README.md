# BigShift

BigShift is a tool for moving tables from Redshift to BigQuery. It will create a table in BigQuery with a schema that matches the Redshift table, dump the data to S3, transfer it to GCS and finally load it into the BigQuery table.

# Installation

```
$ gem install bigshift
```

# Requirements

On the AWS side you need a Redshift cluster and an S3 bucket, and credentials that let you read from Redshift, and read and write to the S3 bucket (it doesn't have to be to the whole bucket, a prefix works fine). On the GCP side you need a Cloud Storage bucket, a BigQuery dataset and credentials that allows reading and writing to the bucket, and create BigQuery tables.

# Usage

The main interface to BigShift is the `bigshift` command line tool.

BigShift can also be used as a library in a Ruby application. Look at the tests, and how the `bigshift` tool is built to figure out how.

## Arguments

Running `bigshift` without any arguments, or with `--help` will show the options. All except `--s3-prefix` are required.

### GCP credentials

The `--gcp-credentials` argument must be a path to a JSON file that contains a public/private key pair for a GCP user. The best way to obtain this is to create a new service account and chose JSON as the key type when prompted.

### AWS credentials

The `--aws-credentials` argument must be a path to a JSON or YAML file that contains `aws_access_key_id` and `aws_secret_access_key`, and optionally `token`.

```yaml
---
aws_access_key_id: AKXYZABC123FOOBARBAZ
aws_secret_access_key: eW91ZmlndXJlZG91dGl0d2FzYmFzZTY0ISEhCg
```

These credentials need to be allowed to read and write the S3 location you specify with `--s3-bucket` and `--s3-prefix`.

### Redshift credentials

The `--rs-credentials` argument must be a path to a JSON or YAML file that contains the `host` and `port` of the Redshift cluster, as well as the `username` and `password` required to connect.

```yaml
---
host: my-cluster.abc123.eu-west-1.redshift.amazonaws.com
port: 5439
username: my_redshift_user
password: dGhpc2lzYWxzb2Jhc2U2NAo
```

# How does it work?

There are four main pieces to BigShift: the Redshift unloader, the transfer, the BigQuery load and the schema translation.

In theory it's pretty simple: the Redshift table is dumped to S3 using Redshift's `UNLOAD` command, copied over to GCS and loaded into BigQuery – but the devil is the details.

The CSV produced by Redshift's `UNLOAD` can't be loaded into BigQuery no matter what options you specify on either end. Redshift can quote _all_ fields or none, but BigQuery doesn't allow non-string fields to be quoted. The format of booleans and timestamps are not compatible, and they expect quotes in quoted fields to be escaped differently, to name a few things.

This means that a lot of what BigShift does is make sure that the data that is dumped from Redshift is compatible with BigQuery. To do this it reads the table schema and translates the different datatypes while the data is dumped. Quotes are escaped, timestamps formatted, and so on.

Once the data is on S3 it's fairly simple to move it over to GCS. GCS has a great service called Transfer Service, that does the transfer for you. If this didn't exist you would have to stream all of the bytes through the machine that ran BigShift. As long as you've set up the credentials right in AWS IAM this works smoothly.

Once the data is in GCS, the BigQuery table can be created and loaded. At this point the Redshift table's schema is translated into a BigQuery schema. The Redshift datatypes are mapped to BigQuery datatypes and things like nullability are determines. The mapping is straighforward:

* `BOOLEAN` in Redshift becomes `BOOLEAN` in BigQuery
* all Redshift integer types are mapped to BigQuery's `INTEGER`
* all Redshift floating point types are mapped to BigQuery's `FLOAT`
* `DATE` in Redshift becomes `STRING` in BigQuery (formatted as YYYY-MM-DD)
* `NUMERIC` is mapped to `STRING`, because BigQuery doesn't have any equivalent data type and using `STRING` avoids loosing precision
* `TIMESTAMP` in Redshift becomes `TIMESTAMP` in BigQuery, and the data is transferred as a UNIX timestamp with fractional seconds (to the limit of what Redshift's `TIMESTAMP` datatype provides)
* `CHAR` and `VARCHAR` obviously become `STRING` in BigQuery

`NOT NULL` becomes `REQUIRED` in BigQuery, and `NULL` becomes `NULLABLE`.

# What doesn't it do?

* Currently BigShift doesn't delete the dumped table from S3 or from GCS. This is planned.
* BigShift can't currently append to an existing BigQuery table. This feature would be possible to add.
* The tool will happily overwrite any data on S3, GCS and in BigQuery that happen to be in the way (i.e. in the specified S3 or GCS location, or in the target table). This is convenient if you want to move the same data multiple times, but very scary and unsafe. To clobber everything will be an option in the future, but the default will be much safer.
* There is no transformation or processing of the data. When moving to BigQuery you might want to split a string and use the pieces as values in a repeated field, but BigShift doesn't help you with that. You will almost always have to do some post processing in BigQuery once the data has been moved. Processing on the way would require a lot more complexity and involve either Hadoop or Dataflow, and that's beyond the scope of a tool like this.
* BigShift can't move data back from BigQuery to Redshift. It can probably be done, but you would probably have to write a big part of the Redshift schema yourself since BigQuery's data model is so much simpler. Going from Redshift to BigQuery is simple, most of Redshifts datatypes map directly to one of BigQuery's, and there's no encodings, sort or dist keys to worry about. Going in the other direction the tool can't know whether or not a `STRING` column in BigQuery should be a `CHAR(12)` or `VARCHAR(65535)`, and if it should be encoded as `LZO` or `BYTEDICT` or what should be the primary, sort, and dist key of the table.

# Troubleshooting

### I get SSL errors

The certificates used by the Google APIs might not be installed on your system, try this as a workaround:

```
export SSL_CERT_FILE="$(find $GEM_HOME/gems -name 'google-api-client-*' | tail -n 1)/lib/cacerts.pem"
```

### I get errors when the data is loaded into BigQuery

This could be anything, but it could be things that aren't escaped properly when the data is dumped from Redshift. Try figuring out from the errors where the problem is and what the data looks like and open an issue. The more you can figure out yourself the more likely it is that you will get help. No one wants to trawl through your data, make an effort.

# Copyright

© 2016 Theo Hultberg and contributors, see LICENSE.txt (BSD 3-Clause).
