--- DDL for tracking cumulative host activity
CREATE TABLE hosts_cumulated (
    host TEXT, -- Unique identifier for the host (e.g., server or system name)
    dates_active DATE[], -- Array of dates in the past when the host was active
    date DATE, -- Current date being tracked for this host
    PRIMARY KEY (host, date) -- Ensures no duplicate entries for the same host and date
);
