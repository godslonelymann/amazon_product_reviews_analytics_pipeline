"""Streamlit dashboard backed by Amazon Athena and Boto3."""

import os
import sys
import time
from pathlib import Path

import altair as alt
import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


# config.py is optional; environment variables always take precedence.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
try:
    import config as local_config
except ImportError:
    local_config = None


def setting(name: str, default: str = "") -> str:
    """Read a setting from the environment, then optional local config.py."""
    return os.getenv(name, getattr(local_config, name, default) if local_config else default)


AWS_REGION = setting("AWS_REGION", "ap-south-1")
ATHENA_DATABASE = setting("ATHENA_DATABASE", "amazon_appliances")
ATHENA_OUTPUT_LOCATION = setting("ATHENA_OUTPUT_LOCATION")
ATHENA_WORKGROUP = setting("ATHENA_WORKGROUP", "primary")
AWS_PROFILE = setting("AWS_PROFILE")


def athena_client():
    """Use Boto3's normal credential chain; no credentials live in this code."""
    session = boto3.Session(
        profile_name=AWS_PROFILE or None,
        region_name=AWS_REGION,
    )
    return session.client("athena")


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """Run Athena SQL, wait for completion, and return all result pages."""
    if not ATHENA_OUTPUT_LOCATION.startswith("s3://"):
        raise ValueError(
            "Set ATHENA_OUTPUT_LOCATION to an S3 URI such as "
            "s3://my-bucket/athena-query-results/."
        )

    client = athena_client()
    response = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_LOCATION},
        WorkGroup=ATHENA_WORKGROUP,
    )
    query_id = response["QueryExecutionId"]

    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        execution = client.get_query_execution(QueryExecutionId=query_id)
        status = execution["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            reason = status.get("StateChangeReason", "No reason returned by Athena")
            raise RuntimeError(f"Athena query {state.lower()}: {reason}")
        time.sleep(1)
    else:
        client.stop_query_execution(QueryExecutionId=query_id)
        raise TimeoutError("Athena query exceeded the 120-second dashboard timeout")

    rows: list[list[str | None]] = []
    columns: list[str] = []
    paginator = client.get_paginator("get_query_results")
    first_page = True
    for page in paginator.paginate(QueryExecutionId=query_id):
        if not columns:
            columns = [
                item["Name"]
                for item in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            ]
        page_rows = page["ResultSet"]["Rows"]
        # Athena repeats column names as the first row of the first page.
        if first_page and page_rows:
            page_rows = page_rows[1:]
            first_page = False
        for row in page_rows:
            values = [item.get("VarCharValue") for item in row.get("Data", [])]
            values.extend([None] * (len(columns) - len(values)))
            rows.append(values)
    return pd.DataFrame(rows, columns=columns)


def to_number(dataframe: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Convert selected Athena string columns to numeric values for charts."""
    conversions = {
        column: pd.to_numeric(dataframe[column], errors="coerce")
        for column in columns
        if column in dataframe.columns
    }
    # assign returns a new dataframe and avoids chained-assignment behaviour.
    return dataframe.assign(**conversions)


def show_dashboard() -> None:
    st.set_page_config(page_title="Amazon Appliances Reviews", layout="wide")
    st.title("Amazon Appliances Reviews")
    st.caption(f"Athena database: {ATHENA_DATABASE} · Region: {AWS_REGION}")

    metrics = to_number(
        run_query(
            """
            SELECT
                SUM(review_count) AS total_reviews,
                SUM(rating_sum) / NULLIF(SUM(rated_review_count), 0) AS average_rating
            FROM monthly_review_summary
            """
        ),
        ["total_reviews", "average_rating"],
    )
    product_total = to_number(
        run_query("SELECT COUNT(*) AS total_products FROM products"),
        ["total_products"],
    )
    total_reviews = int(metrics.iloc[0]["total_reviews"] or 0)
    total_products = int(product_total.iloc[0]["total_products"] or 0)
    average_rating = metrics.iloc[0]["average_rating"]

    col1, col2, col3 = st.columns(3)
    col1.metric("Total reviews", f"{total_reviews:,}")
    col2.metric("Total products", f"{total_products:,}")
    col3.metric(
        "Average rating",
        "N/A" if pd.isna(average_rating) else f"{average_rating:.2f} / 5",
    )

    rating_distribution = to_number(
        run_query(
            """
            SELECT rating, review_count
            FROM (
                SELECT 1 AS rating, SUM(rating_1_count) AS review_count FROM monthly_review_summary
                UNION ALL SELECT 2, SUM(rating_2_count) FROM monthly_review_summary
                UNION ALL SELECT 3, SUM(rating_3_count) FROM monthly_review_summary
                UNION ALL SELECT 4, SUM(rating_4_count) FROM monthly_review_summary
                UNION ALL SELECT 5, SUM(rating_5_count) FROM monthly_review_summary
            )
            ORDER BY rating
            """
        ),
        ["rating", "review_count"],
    )
    rating_total = rating_distribution["review_count"].sum()
    rating_distribution = rating_distribution.assign(
        review_share=(
            rating_distribution["review_count"] / rating_total
            if rating_total
            else 0
        )
    )

    top_products = to_number(
        run_query(
            """
            SELECT product_asin, product_title, brand, review_count,
                   average_review_rating
            FROM product_summary
            WHERE review_count > 0
            ORDER BY review_count DESC
            LIMIT 10
            """
        ),
        ["review_count", "average_review_rating"],
    )
    top_products = top_products.assign(
        product_label=top_products.apply(
            lambda row: (
                f"{str(row['product_title'] or 'Unknown product')[:48]}"
                f" · {str(row['product_asin'])[-6:]}"
            ),
            axis=1,
        )
    )

    left, right = st.columns([1, 2])
    with left:
        st.subheader("Rating distribution")
        st.caption("Share of rated reviews at each star level")
        rating_bars = (
            alt.Chart(rating_distribution)
            .mark_bar()
            .encode(
                x=alt.X("rating:O", title="Stars", sort=[1, 2, 3, 4, 5]),
                y=alt.Y(
                    "review_share:Q",
                    title="Share of reviews",
                    axis=alt.Axis(format=".0%"),
                ),
                tooltip=[
                    alt.Tooltip("rating:O", title="Stars"),
                    alt.Tooltip("review_count:Q", title="Reviews", format=","),
                    alt.Tooltip("review_share:Q", title="Share", format=".1%"),
                ],
            )
        )
        rating_labels = rating_bars.mark_text(dy=-8).encode(
            text=alt.Text("review_share:Q", format=".1%")
        )
        st.altair_chart(
            (rating_bars + rating_labels).properties(height=310),
            use_container_width=True,
        )
    with right:
        st.subheader("Top-reviewed products")
        st.caption("Products with the most reviews in this dataset")
        top_bars = (
            alt.Chart(top_products)
            .mark_bar()
            .encode(
                x=alt.X("review_count:Q", title="Review count"),
                y=alt.Y(
                    "product_label:N",
                    title="Product",
                    sort=alt.EncodingSortField(
                        field="review_count", order="descending"
                    ),
                    axis=alt.Axis(labelLimit=330),
                ),
                tooltip=[
                    "product_title",
                    "product_asin",
                    "brand",
                    alt.Tooltip("review_count:Q", format=","),
                    alt.Tooltip("average_review_rating:Q", format=".2f"),
                ],
            )
        )
        top_labels = top_bars.mark_text(align="left", dx=5).encode(
            text=alt.Text("review_count:Q", format=",")
        )
        st.altair_chart(
            (top_bars + top_labels).properties(height=310),
            use_container_width=True,
        )

    brand_performance = to_number(
        run_query(
            """
            SELECT brand, product_count, review_count, average_review_rating
            FROM brand_summary
            WHERE review_count >= 10
              AND average_review_rating IS NOT NULL
              AND brand <> 'Unknown'
            ORDER BY review_count DESC
            LIMIT 12
            """
        ),
        ["product_count", "review_count", "average_review_rating"],
    )
    brand_order = brand_performance.sort_values(
        "review_count", ascending=False
    )["brand"].tolist()

    st.subheader("Brand performance")
    st.caption(
        "The 12 highest-volume brands: review volume on the left and "
        "customer rating on the same brand order on the right"
    )
    left, right = st.columns(2)
    with left:
        brand_volume_bars = (
            alt.Chart(brand_performance)
            .mark_bar()
            .encode(
                x=alt.X("review_count:Q", title="Review count"),
                y=alt.Y(
                    "brand:N",
                    title=None,
                    sort=brand_order,
                    axis=alt.Axis(labelLimit=260),
                ),
                tooltip=[
                    "brand",
                    alt.Tooltip("review_count:Q", format=","),
                    alt.Tooltip("product_count:Q", format=","),
                    alt.Tooltip("average_review_rating:Q", format=".2f"),
                ],
            )
        )
        brand_volume_labels = brand_volume_bars.mark_text(
            align="left", dx=5
        ).encode(text=alt.Text("review_count:Q", format=","))
        st.altair_chart(
            (brand_volume_bars + brand_volume_labels).properties(height=360),
            use_container_width=True,
        )
    with right:
        rating_dots = (
            alt.Chart(brand_performance)
            .mark_circle(size=95)
            .encode(
                x=alt.X(
                    "average_review_rating:Q",
                    title="Average review rating",
                    scale=alt.Scale(domain=[1, 5]),
                ),
                y=alt.Y("brand:N", title=None, sort=brand_order),
                tooltip=[
                    "brand",
                    alt.Tooltip("average_review_rating:Q", format=".2f"),
                    alt.Tooltip("review_count:Q", format=","),
                    alt.Tooltip("product_count:Q", format=","),
                ],
            )
        )
        if not pd.isna(average_rating):
            overall_rule = (
                alt.Chart(pd.DataFrame({"overall_rating": [average_rating]}))
                .mark_rule(strokeDash=[5, 4])
                .encode(x="overall_rating:Q")
            )
            rating_dots = rating_dots + overall_rule
        st.altair_chart(
            rating_dots.properties(height=360),
            use_container_width=True,
        )

    monthly = to_number(
        run_query(
            """
            SELECT review_month, review_count, average_rating
            FROM monthly_review_summary
            WHERE review_year <> '0'
            ORDER BY review_month
            """
        ),
        ["review_count", "average_rating"],
    )
    monthly = (
        monthly.assign(
            month=pd.to_datetime(monthly["review_month"], errors="coerce")
        )
        .dropna(subset=["month"])
        .sort_values("month")
    )
    monthly = monthly.assign(
        rolling_reviews=monthly["review_count"].rolling(3, min_periods=1).mean()
    )
    monthly_long = monthly.melt(
        id_vars=["month"],
        value_vars=["review_count", "rolling_reviews"],
        var_name="series",
        value_name="reviews",
    ).replace(
        {
            "series": {
                "review_count": "Monthly reviews",
                "rolling_reviews": "3-month average",
            }
        }
    )

    st.subheader("Monthly review trend")
    st.caption(
        "Monthly review counts with a three-month rolling average to reveal "
        "the underlying direction"
    )
    monthly_chart = (
        alt.Chart(monthly_long)
        .mark_line()
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("reviews:Q", title="Review count"),
            color=alt.Color("series:N", title=None),
            strokeWidth=alt.condition(
                alt.datum.series == "3-month average", alt.value(3), alt.value(1)
            ),
            opacity=alt.condition(
                alt.datum.series == "3-month average", alt.value(1), alt.value(0.4)
            ),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%Y-%m"),
                alt.Tooltip("series:N", title="Series"),
                alt.Tooltip("reviews:Q", title="Reviews", format=",.0f"),
            ],
        )
        .properties(height=340)
    )
    st.altair_chart(monthly_chart, use_container_width=True)

    price_bands = to_number(
        run_query(
            """
            WITH priced_products AS (
                SELECT
                    CASE
                        WHEN CAST(price AS DOUBLE) < 25 THEN '$0–24'
                        WHEN CAST(price AS DOUBLE) < 50 THEN '$25–49'
                        WHEN CAST(price AS DOUBLE) < 100 THEN '$50–99'
                        WHEN CAST(price AS DOUBLE) < 250 THEN '$100–249'
                        WHEN CAST(price AS DOUBLE) < 500 THEN '$250–499'
                        WHEN CAST(price AS DOUBLE) < 1000 THEN '$500–999'
                        ELSE '$1,000+'
                    END AS price_band,
                    CASE
                        WHEN CAST(price AS DOUBLE) < 25 THEN 1
                        WHEN CAST(price AS DOUBLE) < 50 THEN 2
                        WHEN CAST(price AS DOUBLE) < 100 THEN 3
                        WHEN CAST(price AS DOUBLE) < 250 THEN 4
                        WHEN CAST(price AS DOUBLE) < 500 THEN 5
                        WHEN CAST(price AS DOUBLE) < 1000 THEN 6
                        ELSE 7
                    END AS price_band_order,
                    average_review_rating,
                    review_count
                FROM product_summary
                WHERE price IS NOT NULL
                  AND CAST(price AS DOUBLE) >= 0
                  AND average_review_rating IS NOT NULL
                  AND review_count >= 5
            )
            SELECT
                price_band,
                price_band_order,
                COUNT(*) AS product_count,
                SUM(review_count) AS review_count,
                AVG(average_review_rating) AS average_rating
            FROM priced_products
            GROUP BY price_band, price_band_order
            ORDER BY price_band_order
            """
        ),
        ["price_band_order", "product_count", "review_count", "average_rating"],
    )
    price_order = price_bands.sort_values("price_band_order")["price_band"].tolist()

    st.subheader("Product price versus average rating")
    st.caption(
        "Average rating by price band; the product-count chart shows how much "
        "evidence supports each average"
    )
    price_count_bars = (
        alt.Chart(price_bands)
        .mark_bar()
        .encode(
            x=alt.X("price_band:N", title=None, sort=price_order),
            y=alt.Y("product_count:Q", title="Products"),
            tooltip=[
                alt.Tooltip("price_band:N", title="Price"),
                alt.Tooltip("product_count:Q", title="Products", format=","),
                alt.Tooltip("review_count:Q", title="Reviews", format=","),
                alt.Tooltip("average_rating:Q", title="Average rating", format=".2f"),
            ],
        )
        .properties(height=150)
    )
    price_count_labels = price_count_bars.mark_text(dy=-8).encode(
        text=alt.Text("product_count:Q", format=",")
    )
    price_rating_line = (
        alt.Chart(price_bands)
        .mark_line(point=alt.OverlayMarkDef(size=80))
        .encode(
            x=alt.X("price_band:N", title="Price band (USD)", sort=price_order),
            y=alt.Y(
                "average_rating:Q",
                title="Average rating",
                scale=alt.Scale(domain=[1, 5]),
            ),
            tooltip=[
                alt.Tooltip("price_band:N", title="Price"),
                alt.Tooltip("average_rating:Q", title="Average rating", format=".2f"),
                alt.Tooltip("product_count:Q", title="Products", format=","),
                alt.Tooltip("review_count:Q", title="Reviews", format=","),
            ],
        )
        .properties(height=220)
    )
    price_chart = alt.vconcat(
        price_count_bars + price_count_labels,
        price_rating_line,
        spacing=12,
    ).resolve_scale(x="shared")
    st.altair_chart(price_chart, use_container_width=True)


try:
    show_dashboard()
except (
    NoCredentialsError,
    BotoCoreError,
    ClientError,
    RuntimeError,
    TimeoutError,
    ValueError,
) as error:
    st.error(str(error))
    st.info(
        "Check AWS credentials, the Athena output S3 location, IAM permissions, "
        "the Glue crawler, and the database/table names."
    )
