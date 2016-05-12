# BigShift

[![Build Status](https://travis-ci.org/iconara/bigshift.png?branch=master)](https://travis-ci.org/iconara/bigshift)

_If you're reading this on GitHub, please note that this is the readme for the development version and that some features described here might not yet have been released. You can find the readme for a specific version either through [rubydoc.info](http://rubydoc.info/find/gems?q=bigshift) or via the release tags ([here is an example](https://github.com/iconara/bigshift/tree/v0.1.1))._

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

Because a transfer can take a long time, it's highly recommended that you run the command in `screen` or `tmux` or using some other mechanism that ensures that the process isn't killed prematurely.

## Cost

Please note that transferring large amounts of data between AWS and GCP is not free. [AWS charges for outgoing traffic from S3](https://aws.amazon.com/s3/pricing/#Data_Transfer_Pricing). There are also storage charges for the Redshift dumps on S3 and GCS, but since they are kept only until the BigQuery table has been loaded those should be negligible.

BigShift tells Redshift to compress the dumps, even if that means that the BigQuery load will be slower, in order to minimize the transfer cost.

## Arguments

Running `bigshift` without any arguments, or with `--help` will show the options. All except `--s3-prefix`, `--bq-table`, `--max-bad-records` and `--steps` are required.

### GCP credentials

The `--gcp-credentials` argument must be a path to a JSON file that contains a public/private key pair for a GCP user. The best way to obtain this is to create a new service account and chose JSON as the key type when prompted.

### AWS credentials

You can provide AWS credentials the same way that you can for the AWS SDK, that is with environment variables and files in specific locations in the file system, etc. See the [AWS SDK documentation](http://aws.amazon.com/documentation/sdk-for-ruby/) for more information. You can't use temporary credentials, like instance role credentials, unfortunately, because GCS Transfer Service doesn't support session tokens.

You can also use the optional `--aws-credentials` argument to point to a JSON or YAML file that contains `access_key_id` and `secret_access_key`, and optionally `region`.

```yaml
---
access_key_id: AKXYZABC123FOOBARBAZ
secret_access_key: eW91ZmlndXJlZG91dGl0d2FzYmFzZTY0ISEhCg
region: eu-west-1
```

These credentials need to be allowed to read and write the S3 location you specify with `--s3-bucket` and `--s3-prefix`.

Here is a minimal IAM policy that should work:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::THE-NAME-OF-THE-BUCKET/THE/PREFIX/*"
      ],
      "Effect": "Allow"
    },
    {
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::THE-NAME-OF-THE-BUCKET"
      ],
      "Effect": "Allow"
    }
  ]
}
```

If you set `THE-NAME-OF-THE-BUCKET` to the same value as `--s3-bucket` and `THE/PREFIX` to the same value as `--s3-prefix` you're limiting the damage that BigShift can do, and unless you store something else at that location there is very little damage to be done.

It is _strongly_ recommended that you create a specific IAM user with minimal permissions for use with BigShift. The nature of GCS Transfer Service means that these credentials are sent to and stored in GCP. The credentials are also used in the `UNLOAD` command sent to Redshift, and with the AWS SDK to work with the objects on S3.

### Redshift credentials

The `--rs-credentials` argument must be a path to a JSON or YAML file that contains the `host` and `port` of the Redshift cluster, as well as the `username` and `password` required to connect.

```yaml
---
host: my-cluster.abc123.eu-west-1.redshift.amazonaws.com
port: 5439
username: my_redshift_user
password: dGhpc2lzYWxzb2Jhc2U2NAo
```

### S3 prefix

If you don't want to put the data dumped from Redshift directly into the root of the S3 bucket you can use the `--s3-prefix` to provide a prefix to where the dumps should be placed.

Because of how GCS' Transfer Service works the transferred files will have exactly the same keys in the destination bucket, this cannot be configured.

### BigQuery table ID

By default the BigQuery table ID will be the same as the Redshift table name, but the optional argument `--bq-table` can be used to tell BigShift to use another table ID.

### Running only some steps

Using the `--steps` argument it's possible to skip some parts of the transfer, or resume a failed transfer. The default is `--steps unload,transfer,load,cleanup`, but using for example `--steps unload,transfer` would dump the table to S3 and transfer the files and then stop.

Another case might be if for some reason the BigShift process was killed during the transfer step. The transfer will still run in GCS, and you might not want to start over from the start, it takes a long time to unload a big table, and an even longer time to transfer it, not to mention bandwidth costs. You can then run the same command again, but add `--steps load,cleanup` to the arguments to skip the unloading and transferring steps.

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

Finally, once the BigQuery table has been loaded BigShift will remove the data it dumped to S3 and the data it transferred to Cloud Storage.

# What doesn't it do?

* BigShift can't currently append to an existing BigQuery table. This feature would be possible to add.
* The tool will truncate the target table before loading the transferred data to it. This is convenient if you want to move the same data multiple times, but can also be considered very scary and unsafe. It would be possible to have options to fail if there is data in the target table, or to append to the target table.
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
