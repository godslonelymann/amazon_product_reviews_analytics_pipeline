-- Ready-to-run analysis examples after the Glue crawler has completed.
USE amazon_appliances;

-- 1. Overall metrics.
SELECT
    SUM(review_count) AS total_reviews,
    SUM(rated_review_count) AS rated_reviews,
    ROUND(SUM(rating_sum) / NULLIF(SUM(rated_review_count), 0), 2)
        AS average_rating
FROM monthly_review_summary;

SELECT COUNT(*) AS total_products
FROM products;

-- 2. Rating distribution. The monthly summary avoids scanning review text.
SELECT rating, review_count
FROM (
    SELECT 1 AS rating, SUM(rating_1_count) AS review_count FROM monthly_review_summary
    UNION ALL
    SELECT 2, SUM(rating_2_count) FROM monthly_review_summary
    UNION ALL
    SELECT 3, SUM(rating_3_count) FROM monthly_review_summary
    UNION ALL
    SELECT 4, SUM(rating_4_count) FROM monthly_review_summary
    UNION ALL
    SELECT 5, SUM(rating_5_count) FROM monthly_review_summary
)
ORDER BY rating;

-- 3. Top-reviewed / most popular products based on review count.
SELECT
    product_asin,
    product_title,
    brand,
    review_count,
    average_review_rating
FROM product_summary
WHERE review_count > 0
ORDER BY review_count DESC
LIMIT 20;

-- 4. Average review rating and product count by brand.
SELECT
    brand,
    product_count,
    review_count,
    average_review_rating,
    average_product_price
FROM brand_summary
WHERE review_count >= 10
ORDER BY review_count DESC
LIMIT 30;

-- 5. Monthly review trend.
SELECT
    review_month,
    review_count,
    average_rating,
    reviewed_product_count
FROM monthly_review_summary
WHERE review_year <> '0'
ORDER BY review_month;

-- 6. Verified versus non-verified reviews.
SELECT purchase_type, review_count
FROM (
    SELECT
        'Verified' AS purchase_type,
        SUM(verified_review_count) AS review_count
    FROM monthly_review_summary
    UNION ALL
    SELECT
        'Non-verified',
        SUM(non_verified_review_count)
    FROM monthly_review_summary
)
ORDER BY review_count DESC;

-- 7. Product price versus average review rating.
SELECT
    product_asin,
    product_title,
    brand,
    CAST(price AS DOUBLE) AS price,
    average_review_rating,
    review_count
FROM product_summary
WHERE price IS NOT NULL
  AND average_review_rating IS NOT NULL
  AND review_count >= 5
ORDER BY review_count DESC
LIMIT 1000;

-- 8. Metadata's historical review count can also identify popular products.
SELECT
    product_asin,
    product_title,
    brand,
    metadata_review_count,
    metadata_average_rating
FROM product_summary
ORDER BY metadata_review_count DESC
LIMIT 20;

-- 9. Limit scans to a review partition when exploring detailed records.
SELECT
    review_month,
    rating,
    is_verified_purchase,
    product_title,
    brand
FROM review_product
WHERE review_year = '2023'
LIMIT 100;
