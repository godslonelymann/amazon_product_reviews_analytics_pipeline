# AWS setup guide

This guide uses AWS Region `ap-south-1`. Replace `YOUR_BUCKET` with one globally
unique, lowercase S3 bucket name. Do not put access keys in this project.

## 1. Create the S3 bucket and folders

In **S3 → Create bucket**:

1. Choose a unique name and Region **Asia Pacific (Mumbai) ap-south-1**.
2. Leave **Block all public access** enabled.
3. Enable default encryption (SSE-S3 is enough for this learning project).
4. Create the bucket.

S3 does not require real folders. They appear when files are uploaded. The job
will create the curated and analytics prefixes automatically.

The equivalent CLI command is:

```bash
aws s3api create-bucket \
  --bucket YOUR_BUCKET \
  --region ap-south-1 \
  --create-bucket-configuration LocationConstraint=ap-south-1
```

## 2. Credentials and permissions

For local work, run `aws configure` or use environment variables. On AWS, use an
IAM role. The code uses the standard AWS credential provider chains in Spark and
Boto3.

The person or role running the batch job needs these actions on the project
bucket:

- `s3:ListBucket`
- `s3:GetObject`
- `s3:PutObject`
- `s3:DeleteObject` (overwrite mode removes old output files)

The dashboard identity also needs:

- `athena:StartQueryExecution`
- `athena:GetQueryExecution`
- `athena:GetQueryResults`
- `athena:StopQueryExecution`
- `glue:GetDatabase`, `glue:GetTable`, and `glue:GetPartitions`
- Read/write access to the Athena results prefix

For a Glue crawler, create an IAM service role such as
`AWSGlueServiceRole-AmazonAppliancesCrawler`. Attach the AWS managed
`AWSGlueServiceRole` policy and grant read access to these two prefixes:

```text
s3://YOUR_BUCKET/curated/
s3://YOUR_BUCKET/analytics/
```

Keep permissions limited to this bucket when possible.

## 3. Upload the raw JSONL files

Download the real Amazon Reviews 2023 Appliances review and metadata JSONL files
from the McAuley Lab dataset. Then upload them as follows:

```bash
aws s3 cp Appliances.jsonl.gz s3://YOUR_BUCKET/raw/reviews/Appliances.jsonl.gz
aws s3 cp meta_Appliances.jsonl.gz s3://YOUR_BUCKET/raw/metadata/meta_Appliances.jsonl.gz
```

Confirm the upload:

```bash
aws s3 ls s3://YOUR_BUCKET/raw/reviews/
aws s3 ls s3://YOUR_BUCKET/raw/metadata/
```

## 4. Run the PySpark batch job

The README shows the local `spark-submit` command. For an AWS-managed run, you
may use one AWS Glue Spark job; this is optional and does not change the code:

1. Upload `spark_jobs/process_amazon_data.py` to a scripts prefix in the bucket.
2. Open **AWS Glue → ETL jobs → Script editor → Upload script**.
3. Choose a Spark job with Python 3 and Glue 4.0 or newer.
4. Select or create a Glue job role with read/write access to the project bucket.
5. Add job parameter `--bucket` with value `YOUR_BUCKET`.
6. Start with two `G.1X` workers for the full Appliances files. Run the job and
   inspect its CloudWatch logs.

Only one Spark job is needed. It writes all six Parquet datasets.

## 5. Create the Glue database and crawler

### Create the database

1. Open **AWS Glue → Data Catalog → Databases → Add database**.
2. Enter `amazon_appliances`.
3. Choose **Create database**.

### Create one crawler

1. Open **AWS Glue → Crawlers → Create crawler**.
2. Name it `amazon-appliances-parquet-crawler`.
3. Choose **Not yet** for an existing Data Catalog table.
4. Add these six S3 data sources, one target per final dataset:

   ```text
   s3://YOUR_BUCKET/curated/reviews/
   s3://YOUR_BUCKET/curated/products/
   s3://YOUR_BUCKET/curated/review_product/
   s3://YOUR_BUCKET/analytics/brand_summary/
   s3://YOUR_BUCKET/analytics/product_summary/
   s3://YOUR_BUCKET/analytics/monthly_review_summary/
   ```

5. Choose the crawler IAM role created earlier.
6. Use **On demand** frequency.
7. Select the `amazon_appliances` target database and leave the table prefix
   empty.
8. In advanced grouping options, select **Create a single schema for each S3
   path** so the six targets do not get combined.
9. Create and run the crawler.

The expected tables are:

```text
reviews
products
review_product
brand_summary
product_summary
monthly_review_summary
```

The crawler should discover `review_year` as a partition in `reviews`,
`review_product`, and `monthly_review_summary`. Other tables are deliberately not
partitioned. Run the crawler again whenever the output schema or partitions
change.

If Glue generates different names, rename the tables in Glue or update the SQL
and dashboard queries to use the generated names.

## 6. Configure Athena

1. Open **Amazon Athena → Query editor** and select `ap-south-1`.
2. Choose **Settings → Manage**.
3. Set the query result location to:

   ```text
   s3://YOUR_BUCKET/athena-query-results/
   ```

4. Keep the `primary` workgroup, or update `ATHENA_WORKGROUP` if using another.
5. Select the `AwsDataCatalog` data source and the `amazon_appliances` database.
6. Run `athena/validation_queries.sql`, one statement at a time.
7. Try the examples in `athena/analytics_queries.sql`.

Athena charges by bytes scanned. The Parquet outputs and partition filters reduce
the scan size. Queries against `review_product` should include `review_year` when
possible.

## 7. Configure the dashboard

Either copy the example configuration:

```bash
cp config.example.py config.py
```

and edit the bucket name, or set environment variables:

```bash
export AWS_REGION=ap-south-1
export ATHENA_DATABASE=amazon_appliances
export ATHENA_OUTPUT_LOCATION=s3://YOUR_BUCKET/athena-query-results/
export ATHENA_WORKGROUP=primary
```

For a named AWS CLI profile without environment exports, add this to
`config.py`:

```python
AWS_PROFILE = "amazon-appliances"
```

The dashboard requires no access key in its source code. An IAM role is the best
choice when it runs on AWS.

## 8. Cleanup

To stop charges after the project:

1. Stop any running Glue job.
2. Delete the Glue crawler and Data Catalog tables/database.
3. Empty and delete the S3 bucket. This removes raw data, Parquet output, scripts,
   and Athena query results.
4. Delete IAM roles created only for this project.

CLI cleanup (this permanently deletes the bucket contents):

```bash
aws s3 rm s3://YOUR_BUCKET --recursive
aws s3api delete-bucket --bucket YOUR_BUCKET --region ap-south-1
```
