INSERT INTO actors_history_scd

WITH

streak_started AS (
SELECT actor,
       current_year,
       quality_class,
       is_active,
       LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) <> quality_class
               OR LAG(quality_class, 1) OVER
               (PARTITION BY actor ORDER BY current_year) IS NULL
               AS quality_class_did_change,
       LAG(is_active, 1) OVER
                (PARTITION BY actor ORDER BY current_year) <> is_active
                OR LAG(is_active, 1) OVER
                (PARTITION BY actor ORDER BY current_year) IS NULL
                AS is_active_did_change      
FROM actors
),

streak_started_flags AS (
SELECT
	ss.*,
	CASE
		WHEN quality_class_did_change THEN 1
		WHEN is_active_did_change THEN 1
		ELSE 0
	END change_indicator
FROM streak_started ss
),

streak_identified AS (
SELECT
	actor,
	quality_class,
	is_active,
	current_year,
	SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
FROM streak_started_flags
),

aggregated AS (
SELECT
            actor,
            quality_class,
            is_active,
            streak_identifier,
            MIN(current_year) AS start_date,
            MAX(current_year) AS end_date
         FROM streak_identified
         GROUP BY 1,2,3,4
     )

SELECT
	actor,
	quality_class,
	is_active,
	start_date,
	end_date
FROM aggregated
ORDER BY actor, start_date
;

