-- Inserts cumulative host activity data into `hosts_cumulated`
INSERT INTO hosts_cumulated

WITH

-- Retrieves the cumulative activity data from the previous day (2022-12-31)
yesterday AS (
    SELECT
        * -- Includes all columns (host, dates_active, date)
    FROM hosts_cumulated
    WHERE 1=1
        AND date = DATE('2022-12-31') -- Filters for records with the previous date
),

-- Extracts today's activity data from the `events` table for the target date (2023-01-01)
today AS (
    SELECT
        host, -- Host identifier for today's activity
        DATE(event_time) AS date_active -- The date when the host was active
    FROM events e
    WHERE DATE(e.event_time) = DATE('2023-01-01') -- Filters events for the target date
        AND e.host IS NOT NULL -- Ensures the host field is not null
    GROUP BY 1, 2 -- Groups by host and active date to avoid duplicate entries
),

-- Combines yesterday's and today's data to generate the cumulative results
results AS (
    SELECT
        -- Combines host data from today and yesterday
        COALESCE(t.host, y.host) AS host,
        -- Updates the `dates_active` column:
        -- 1. If no previous activity, initializes with today's date
        -- 2. If no current activity, retains yesterday's active dates
        -- 3. Otherwise, appends today's date to the existing dates
        CASE
            WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
            WHEN t.date_active IS NULL THEN y.dates_active
            ELSE ARRAY[t.date_active] || y.dates_active
        END AS dates_active,
        -- Updates the `date` column:
        -- 1. Uses today's active date if present
        -- 2. Otherwise, increments yesterday's date by one day
        COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
    FROM today t
    FULL OUTER JOIN yesterday y -- Includes all hosts from both today and yesterday
    ON t.host = y.host -- Matches records by host identifier
)

-- Inserts distinct records from the cumulative results into the target table
SELECT DISTINCT * FROM results;

