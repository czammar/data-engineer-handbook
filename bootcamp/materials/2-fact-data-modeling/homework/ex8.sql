/**
    An incremental query to load data into the `host_activity_reduced` table.
    Purpose:
    - Processes daily metrics (site hits and unique visitors) for each host.
    - Updates monthly arrays in `host_activity_reduced`, appending new daily metrics.

    Key Features:
    - Uses array operations to build or extend arrays day-by-day.
    - Ensures missing days are accounted for with padding (using `ARRAY_FILL`).
    - Handles potential null values using `COALESCE`.
**/

INSERT INTO host_activity_reduced

WITH

-- Aggregates daily metrics (site hits and unique visitors) for the target date
daily_aggregate AS (
    SELECT 
        host, -- Host identifier
        DATE(event_time) AS date, -- The specific day being processed
        COUNT(1) AS num_site_hits, -- Total number of site hits for the day
        CARDINALITY(array_agg(DISTINCT user_id::TEXT)) AS unique_visitors -- Count of unique visitors for the day
    FROM events e
    WHERE DATE(event_time) = DATE('2023-01-01') -- Filters for the target date
        AND host IS NOT NULL -- Excludes records without a valid host
        AND user_id IS NOT NULL -- Excludes records without a valid user_id
    GROUP BY host, DATE(event_time) -- Groups by host and date for daily aggregation
),

-- Retrieves the previous day's array metrics for the target month
yesterday_array AS (
    SELECT
        host_array_metrics.*
    FROM host_array_metrics
    WHERE month_start = DATE('2023-01-01') -- Filters for the current month's metrics
),

-- Processes `site_hits` metric, appending new daily values to the array
metric_site_hits AS (
    SELECT 
        COALESCE(da.host, ya.host) AS host, -- Merges hosts from both daily and previous data
        COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start, -- Ensures the correct month is set
        'site_hits' AS metric_name, -- Specifies the metric name
        -- Updates the metric array:
        -- 1. If a previous array exists, appends today's value to it.
        -- 2. If no array exists, fills missing days with zeros and starts the array with today's value.
        CASE 
            WHEN ya.metric_array IS NOT NULL THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
            WHEN ya.metric_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
        END AS metric_array
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
    ON ya.host = da.host -- Joins on the host to match daily data with previous month's data
),

-- Processes `unique_visitors` metric, appending new daily values to the array
metric_unique_visitors AS (
    SELECT 
        COALESCE(da.host, ya.host) AS host, -- Merges hosts from both daily and previous data
        COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start, -- Ensures the correct month is set
        'unique_visitors' AS metric_name, -- Specifies the metric name
        -- Updates the metric array:
        -- 1. If a previous array exists, appends today's value to it.
        -- 2. If no array exists, fills missing days with zeros and starts the array with today's value.
        CASE 
            WHEN ya.metric_array IS NOT NULL THEN ya.metric_array || ARRAY[COALESCE(da.unique_visitors, 0)]
            WHEN ya.metric_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.unique_visitors, 0)]
        END AS metric_array
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
    ON ya.host = da.host -- Joins on the host to match daily data with previous month's data
),

-- Combines results for both metrics into a single dataset
results AS (
    SELECT * FROM metric_site_hits
    UNION ALL
    SELECT * FROM metric_unique_visitors
)

-- Inserts or updates the `host_activity_reduced` table
SELECT * FROM results
ON CONFLICT (host, month_start, metric_name)
DO
    UPDATE SET metric_array = EXCLUDED.metric_array; -- Updates the array with new metrics
