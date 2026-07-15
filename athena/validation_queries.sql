-- Run the Glue crawler before these queries.
-- Change the database name here if you chose a different one.
USE amazon_appliances;

-- 1. Confirm that all expected tables exist.
SHOW TABLES;

-- 2. Row counts for the curated tables.
SELECT 'reviews' AS table_name, COUNT(*) AS row_count FROM reviews
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'review_product', COUNT(*) FROM review_product;

-- 3. No curated record should have a missing product ID.
SELECT 'reviews' AS table_name, COUNT(*) AS missing_product_ids
FROM reviews
WHERE product_asin IS NULL OR TRIM(product_asin) = ''
UNION ALL
SELECT 'products', COUNT(*)
FROM products
WHERE product_asin IS NULL OR TRIM(product_asin) = '';

-- 4. Review IDs and product IDs should be unique in their own datasets.
SELECT
    COUNT(*) - COUNT(DISTINCT review_id) AS duplicate_review_count
FROM reviews;

SELECT
    COUNT(*) - COUNT(DISTINCT product_asin) AS duplicate_product_count
FROM products;

-- 5. Check data-type rules. Each result should be zero.
SELECT
    SUM(CASE WHEN rating < 1 OR rating > 5 THEN 1 ELSE 0 END) AS invalid_ratings,
    SUM(CASE WHEN helpful_vote_count < 0 THEN 1 ELSE 0 END) AS invalid_helpful_votes
FROM reviews;

SELECT
    SUM(CASE WHEN price < 0 THEN 1 ELSE 0 END) AS invalid_prices,
    SUM(CASE WHEN metadata_review_count < 0 THEN 1 ELSE 0 END) AS invalid_review_counts
FROM products;

-- 6. See how many reviews did not find matching product metadata.
SELECT
    COUNT(*) AS total_reviews,
    SUM(CASE WHEN product_title IS NULL THEN 1 ELSE 0 END) AS unmatched_reviews,
    ROUND(
        100.0 * SUM(CASE WHEN product_title IS NULL THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS unmatched_percent
FROM review_product;

-- 7. Inspect the small year=0 partition used for invalid/missing timestamps.
SELECT review_year, COUNT(*) AS review_count
FROM reviews
GROUP BY review_year
ORDER BY review_year;

-- 8. Confirm that analytics totals agree with all curated reviews.
SELECT
    (SELECT COUNT(*) FROM reviews) AS curated_reviews,
    (SELECT SUM(review_count) FROM monthly_review_summary)
        AS monthly_summary_reviews;
