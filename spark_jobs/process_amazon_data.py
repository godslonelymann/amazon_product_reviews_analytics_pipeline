#!/usr/bin/env python3
"""Clean Amazon Appliances JSONL data and write curated Parquet datasets."""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


REGION = "ap-south-1"

# Explicit schemas keep Spark from inferring thousands of unused fields inside
# metadata.details. That free-form object contains keys that differ only by case
# (for example "Assembly Required"), which can trigger COLUMN_ALREADY_EXISTS.
REVIEW_SCHEMA = StructType(
    [
        StructField("rating", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("parent_asin", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("verified_purchase", BooleanType(), True),
        StructField("helpful_vote", LongType(), True),
    ]
)

PRODUCT_SCHEMA = StructType(
    [
        StructField("parent_asin", StringType(), True),
        StructField("title", StringType(), True),
        StructField("store", StringType(), True),
        StructField("main_category", StringType(), True),
        StructField("price", StringType(), True),
        StructField("average_rating", DoubleType(), True),
        StructField("rating_number", LongType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bucket",
        default=os.getenv("DATA_LAKE_BUCKET"),
        help="S3 bucket name. You may instead set DATA_LAKE_BUCKET.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_REGION", REGION),
        help=f"AWS Region (default: {REGION}).",
    )
    # AWS Glue adds its own arguments (for example --JOB_NAME), so ignore extras.
    args, _ = parser.parse_known_args()
    if not args.bucket:
        raise ValueError(
            "Missing bucket name. In Glue Job details > Advanced properties > "
            "Job parameters, add --bucket with the S3 bucket name as its value."
        )
    if args.bucket.startswith("s3://"):
        raise ValueError(
            "The --bucket value must be only the bucket name, without s3://."
        )
    return args


def create_spark(region: str) -> SparkSession:
    """Create one Spark session for the complete batch pipeline."""
    return (
        SparkSession.builder.appName("AmazonAppliancesDataLake")
        # This mapping lets local Spark use normal s3:// paths through S3A.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def valid_rating(column: F.Column) -> F.Column:
    """Convert values from 1 through 5 to doubles; return null otherwise."""
    value = column.cast("double")
    return F.when(value.between(1.0, 5.0), value)


def non_negative_long(column: F.Column, default: int = 0) -> F.Column:
    """Safely convert a count to a non-negative long."""
    value = column.cast("long")
    return F.when(value >= 0, value).otherwise(F.lit(default).cast("long"))


def clean_reviews(raw: DataFrame) -> DataFrame:
    """Select, rename, type, validate, and deduplicate review records."""
    timestamp_ms = F.col("timestamp").cast("long")
    review_timestamp = F.when(
        timestamp_ms > 0,
        F.to_timestamp(F.from_unixtime(timestamp_ms / F.lit(1000))),
    )

    reviews = (
        raw.select(
            # The dataset documentation says parent_asin is the metadata join key.
            F.trim(F.coalesce(F.col("parent_asin"), F.col("asin"))).alias(
                "product_asin"
            ),
            F.col("asin").alias("variant_asin"),
            F.col("user_id"),
            valid_rating(F.col("rating")).alias("rating"),
            F.col("title").cast("string").alias("review_title"),
            F.col("text").cast("string").alias("review_text"),
            review_timestamp.alias("review_timestamp"),
            F.coalesce(F.col("verified_purchase").cast("boolean"), F.lit(False)).alias(
                "is_verified_purchase"
            ),
            non_negative_long(F.col("helpful_vote")).alias("helpful_vote_count"),
        )
        .filter(F.col("product_asin").isNotNull())
        .filter(F.length(F.trim(F.col("product_asin"))) > 0)
    )

    # The source has no review ID, so create a stable ID from review attributes.
    reviews = reviews.withColumn(
        "review_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("product_asin"), F.lit("")),
                F.coalesce(F.col("user_id"), F.lit("")),
                F.coalesce(F.col("review_timestamp").cast("string"), F.lit("")),
                F.coalesce(F.col("review_title"), F.lit("")),
                F.coalesce(F.col("review_text"), F.lit("")),
            ),
            256,
        ),
    ).dropDuplicates(["review_id"])

    # Unknown timestamps stay in a small year=0 partition instead of being lost.
    return (
        reviews.withColumn(
            "review_year", F.coalesce(F.year("review_timestamp"), F.lit(0))
        )
        .withColumn(
            "review_month",
            F.coalesce(F.date_format("review_timestamp", "yyyy-MM"), F.lit("unknown")),
        )
        .select(
            "review_id",
            "product_asin",
            "variant_asin",
            "user_id",
            "rating",
            "review_title",
            "review_text",
            "review_timestamp",
            "review_month",
            "is_verified_purchase",
            "helpful_vote_count",
            "review_year",
        )
    )


def clean_products(raw: DataFrame) -> DataFrame:
    """Select useful product fields and safely type price/rating/count values."""
    price_text = F.regexp_replace(F.trim(F.col("price").cast("string")), r"[$,]", "")
    price_number = price_text.cast(DecimalType(10, 2))
    safe_price = F.when(
        price_text.rlike(r"^[0-9]+(\.[0-9]{1,2})?$") & (price_number >= 0),
        price_number,
    )

    products = (
        raw.select(
            F.trim(F.col("parent_asin")).alias("product_asin"),
            F.col("title").cast("string").alias("product_title"),
            F.coalesce(
                F.col("store").cast("string"), F.lit("Unknown")
            ).alias("brand"),
            F.col("main_category").cast("string").alias("main_category"),
            safe_price.alias("price"),
            valid_rating(F.col("average_rating")).alias("metadata_average_rating"),
            non_negative_long(F.col("rating_number")).alias(
                "metadata_review_count"
            ),
        )
        .filter(F.col("product_asin").isNotNull())
        .filter(F.length(F.trim(F.col("product_asin"))) > 0)
        .withColumn(
            "brand",
            F.when(F.length(F.trim("brand")) > 0, F.trim("brand")).otherwise(
                F.lit("Unknown")
            ),
        )
        .dropDuplicates(["product_asin"])
    )
    return products


def create_review_product(reviews: DataFrame, products: DataFrame) -> DataFrame:
    """Add product attributes to each review while keeping unmatched reviews."""
    product_columns = products.select(
        "product_asin",
        "product_title",
        "brand",
        "main_category",
        "price",
        "metadata_average_rating",
        "metadata_review_count",
    )
    return reviews.join(product_columns, "product_asin", "left")


def create_product_summary(review_product: DataFrame, products: DataFrame) -> DataFrame:
    """Create one row per product for ranking and price/rating analysis."""
    review_stats = review_product.groupBy("product_asin").agg(
        F.count("review_id").alias("review_count"),
        F.count("rating").alias("rated_review_count"),
        F.round(F.avg("rating"), 3).alias("average_review_rating"),
        F.sum(F.when(F.col("is_verified_purchase"), 1).otherwise(0)).cast("long").alias(
            "verified_review_count"
        ),
        F.sum(F.when(~F.col("is_verified_purchase"), 1).otherwise(0)).cast("long").alias(
            "non_verified_review_count"
        ),
        F.sum("helpful_vote_count").cast("long").alias("helpful_vote_count"),
    )

    return (
        products.join(review_stats, "product_asin", "full")
        .withColumn("brand", F.coalesce("brand", F.lit("Unknown")))
        .fillna(
            0,
            subset=[
                "review_count",
                "rated_review_count",
                "verified_review_count",
                "non_verified_review_count",
                "helpful_vote_count",
            ],
        )
        .select(
            "product_asin",
            "product_title",
            "brand",
            "main_category",
            "price",
            "metadata_average_rating",
            "metadata_review_count",
            "review_count",
            "rated_review_count",
            "average_review_rating",
            "verified_review_count",
            "non_verified_review_count",
            "helpful_vote_count",
        )
    )


def create_brand_summary(
    review_product: DataFrame, products: DataFrame
) -> DataFrame:
    """Combine product counts and review performance at brand level."""
    product_stats = products.groupBy("brand").agg(
        F.countDistinct("product_asin").alias("product_count"),
        F.round(F.avg("price"), 2).alias("average_product_price"),
    )
    review_stats = (
        review_product.withColumn("brand", F.coalesce("brand", F.lit("Unknown")))
        .groupBy("brand")
        .agg(
            F.count("review_id").alias("review_count"),
            F.count("rating").alias("rated_review_count"),
            F.round(F.avg("rating"), 3).alias("average_review_rating"),
            F.sum(F.when(F.col("is_verified_purchase"), 1).otherwise(0))
            .cast("long")
            .alias("verified_review_count"),
        )
    )
    return product_stats.join(review_stats, "brand", "full").fillna(
        0,
        subset=[
            "product_count",
            "review_count",
            "rated_review_count",
            "verified_review_count",
        ],
    )


def create_monthly_summary(review_product: DataFrame) -> DataFrame:
    """Create monthly trends plus compact rating and verification measures."""
    return review_product.groupBy("review_year", "review_month").agg(
        F.count("review_id").alias("review_count"),
        F.count("rating").alias("rated_review_count"),
        F.round(F.sum("rating"), 3).alias("rating_sum"),
        F.round(F.avg("rating"), 3).alias("average_rating"),
        F.sum(F.when(F.col("rating") == 1, 1).otherwise(0)).cast("long").alias(
            "rating_1_count"
        ),
        F.sum(F.when(F.col("rating") == 2, 1).otherwise(0)).cast("long").alias(
            "rating_2_count"
        ),
        F.sum(F.when(F.col("rating") == 3, 1).otherwise(0)).cast("long").alias(
            "rating_3_count"
        ),
        F.sum(F.when(F.col("rating") == 4, 1).otherwise(0)).cast("long").alias(
            "rating_4_count"
        ),
        F.sum(F.when(F.col("rating") == 5, 1).otherwise(0)).cast("long").alias(
            "rating_5_count"
        ),
        F.sum(F.when(F.col("is_verified_purchase"), 1).otherwise(0)).cast("long").alias(
            "verified_review_count"
        ),
        F.sum(F.when(~F.col("is_verified_purchase"), 1).otherwise(0)).cast("long").alias(
            "non_verified_review_count"
        ),
        F.countDistinct("product_asin").alias("reviewed_product_count"),
    )


def write_parquet(
    dataframe: DataFrame, path: str, partition_columns: list[str] | None = None
) -> None:
    """Write a dataframe as overwrite-mode Snappy Parquet."""
    writer = dataframe.write.mode("overwrite").option("compression", "snappy")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(path)


def run_pipeline(spark: SparkSession, bucket: str) -> None:
    base = f"s3://{bucket}"
    review_input = f"{base}/raw/reviews/"
    metadata_input = f"{base}/raw/metadata/"
    logging.info("Reading review JSONL from %s", review_input)
    raw_reviews = (
        spark.read.schema(REVIEW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(review_input)
    )
    logging.info("Reading metadata JSONL from %s", metadata_input)
    raw_products = (
        spark.read.schema(PRODUCT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(metadata_input)
    )

    reviews = clean_reviews(raw_reviews)
    products = clean_products(raw_products)
    review_product = create_review_product(reviews, products)
    product_summary = create_product_summary(review_product, products)
    brand_summary = create_brand_summary(review_product, products)
    monthly_summary = create_monthly_summary(review_product)

    outputs = [
        (reviews, f"{base}/curated/reviews/", ["review_year"]),
        (products, f"{base}/curated/products/", None),
        (review_product, f"{base}/curated/review_product/", ["review_year"]),
        (brand_summary, f"{base}/analytics/brand_summary/", None),
        (product_summary, f"{base}/analytics/product_summary/", None),
        (
            monthly_summary,
            f"{base}/analytics/monthly_review_summary/",
            ["review_year"],
        ),
    ]
    for dataframe, path, partitions in outputs:
        logging.info("Writing Parquet to %s", path)
        write_parquet(dataframe, path, partitions)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    spark = None
    try:
        args = parse_args()
        logging.info("Starting job for bucket=%s region=%s", args.bucket, args.region)
        spark = create_spark(args.region)
        spark.sparkContext.setLogLevel("WARN")
        run_pipeline(spark, args.bucket)
        logging.info("Amazon Appliances batch job completed successfully")
    except Exception:
        logging.exception("Amazon Appliances batch job failed")
        # Preserve the original exception so AWS Glue shows the useful cause
        # instead of replacing it with the unhelpful "SystemExit: 1" message.
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
