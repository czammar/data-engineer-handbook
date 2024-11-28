-- A cumulative query to generate `device_activity_datelist` from `events`

INSERT INTO users_browser_cumulated

WITH

-- Creates a row number for each unique device and browser combination
devices AS (
    SELECT 
        device_id,
        browser_type,
        ROW_NUMBER() OVER(PARTITION BY device_id, browser_type) AS row_num
    FROM devices d
),

-- Deduplicates devices by selecting the first record for each device and browser combination
devices_dedup AS (
    SELECT 
        device_id,
        browser_type
    FROM devices
    WHERE row_num = 1
),

-- Selects user activity recorded for the previous day (2022-12-31)
yesterday AS (
    SELECT
        *
    FROM users_browser_cumulated
    WHERE date = DATE('2022-12-31')
),

-- Retrieves device activity from events occurring on the target day (2023-01-01)
today_device AS (
    SELECT
        user_id::TEXT, -- User identifier
        e.device_id, -- Device identifier associated with the event
        DATE(event_time) AS date_active, -- The date of the event
        devices_dedup.browser_type -- The browser type used during the event
    FROM events e
    -- Joins with deduplicated devices to get the browser type
    LEFT JOIN devices_dedup
    ON devices_dedup.device_id = e.device_id
    WHERE DATE(e.event_time) = DATE('2023-01-01') -- Filters events for the target date
        AND e.user_id IS NOT NULL -- Ensures the user_id is present
        AND e.device_id IS NOT NULL -- Ensures the device_id is present
),

-- Aggregates today's activity data by user, browser, and active date
today AS (
    SELECT
        user_id,
        browser_type,
        date_active
    FROM today_device device_data
    GROUP BY 1, 2, 3
),

-- Combines today's and yesterday's data to generate cumulative results
results AS (
    SELECT
        COALESCE(t.user_id, y.user_id) AS user_id, -- Combines user_id from today and yesterday
        COALESCE(t.browser_type, y.browser_type) AS browser_type, -- Combines browser_type from today and yesterday
        CASE
            -- If no previous activity, create a new array with today's date
            WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
            -- If no current activity, carry over previous dates
            WHEN t.date_active IS NULL THEN y.dates_active
            -- Otherwise, append today's date to the previous dates
            ELSE ARRAY[t.date_active] || y.dates_active
        END AS dates_active,
        -- Updates the date column with today's date or increments yesterday's date
        COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
    FROM today t
    -- Full outer join to include all records from both today and yesterday
    FULL OUTER JOIN yesterday y
    ON t.user_id = y.user_id
        AND t.browser_type = y.browser_type
)

-- Inserts the results into the target table
SELECT * FROM results;
