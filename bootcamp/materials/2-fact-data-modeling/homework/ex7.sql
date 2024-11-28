/**
    A monthly, reduced fact table `host_activity_reduced`
    Purpose:
    - Tracks aggregated metrics for each host on a monthly basis.
    - Stores metrics in arrays for compact storage and quick retrieval of time-series data.

    Schema:
    - `host`: Identifier for the host.
    - `month_start`: Start date of the month for the aggregated metrics.
    - `metric_name`: Name of the metric being tracked (e.g., "hits", "unique_visitors").
    - `metric_array`: Array of values representing the metric (e.g., daily counts for the month).
**/
CREATE TABLE host_activity_reduced (
    host TEXT, -- Identifier for the host (e.g., server or system name)
    month_start DATE, -- Start date of the month being aggregated (e.g., "2023-01-01")
    metric_name TEXT, -- Name of the metric being tracked (e.g., "hit_array" or "unique_visitors")
    metric_array REAL[], -- Array of numeric values representing the metric for each day of the month
    PRIMARY KEY (host, month_start, metric_name) -- Ensures unique entries for each host, month, and metric
);
