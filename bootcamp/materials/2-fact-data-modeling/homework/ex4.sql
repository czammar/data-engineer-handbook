/**
    A query to generate a `datelist_int` column by converting the `device_activity_datelist` 
    into a compressed 31-bit integer representation for efficient storage and processing.
**/

WITH

-- Prepares the data for calculating activity flags and days since the current date
starter AS (
    SELECT
        -- Checks if the valid_date exists in the `dates_active` array
        ubc.dates_active @> ARRAY[DATE(d.valid_date)] AS is_active,
        -- Calculates the difference in days from the target date (2023-01-31)
        EXTRACT(DAY FROM DATE('2023-01-31') - d.valid_date) AS days_since,
        -- User and browser information from the source table
        ubc.user_id,
        ubc.browser_type
    FROM users_browser_cumulated ubc
    -- Generates a series of dates for the entire month of January 2023
    CROSS JOIN (
        SELECT generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS valid_date
    ) AS d
    -- Filters data for users and browsers active on the target date (2023-01-31)
    WHERE ubc.date = DATE('2023-01-31')
),

-- Builds a 31-bit representation of user activity for the last 31 days
bits AS (
    SELECT
        user_id, -- User identifier
        browser_type, -- Browser type identifier
        -- Constructs a compressed 31-bit integer representing activity for each day
        -- Each bit corresponds to whether the user was active on a specific day
        SUM(
            CASE 
                WHEN is_active 
                THEN POW(2, 1 + 31 - days_since) -- Sets the bit for the specific day
                ELSE 0 
            END
        )::bigint::bit(31) AS datelist_int -- Converts the sum to a 31-bit integer
    FROM starter
    GROUP BY user_id, browser_type -- Groups data by user and browser for aggregation
)

-- Final output includes the compressed activity data and additional metrics
SELECT
    user_id, -- User identifier
    datelist_int, -- 31-bit compressed activity representation
    -- Determines if the user was active at least once during the month
    BIT_COUNT(datelist_int) > 0 AS monthly_active,
    -- Counts the total number of days the user was active in the last 31 days
    BIT_COUNT(datelist_int) AS l31
FROM bits;
