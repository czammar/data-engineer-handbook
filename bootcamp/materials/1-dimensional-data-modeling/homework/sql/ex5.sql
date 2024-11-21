CREATE TYPE actor_scd_type AS (
                    quality_class quality_class_rate,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                    );

-- DROP TYPE actor_scd_type;

INSERT INTO actors_history_scd

WITH last_season_scd AS (
    SELECT * FROM actors_history_scd
    WHERE start_date = 2020
    AND end_date = 2020
),
     historical_scd AS (
        SELECT
            actor,
               quality_class,
               is_active,
               start_date,
               end_date
        FROM actors_history_scd
        WHERE start_date = 2020
        AND end_date < 2020
     ),
     this_season_data AS (
         SELECT * FROM actors
         WHERE current_year = 2021
     ),
     unchanged_records AS (
         SELECT
                ts.actor,
                ts.quality_class,
                ts.is_active,
                ls.start_date,
                ts.current_year as end_season
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.actor = ts.actor
         WHERE ts.quality_class = ls.quality_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.actor,
                UNNEST(ARRAY[
                    ROW(
                        ls.quality_class,
                        ls.is_active,
                        ls.start_date,
                        ls.end_date

                        )::actor_scd_type,
                    ROW(
                        ts.quality_class,
                        ts.is_active,
                        ts.current_year,
                        ts.current_year
                        )::actor_scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.actor = ts.actor
         WHERE (ts.quality_class <> ls.quality_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT actor,
                (records::actor_scd_type).quality_class,
                (records::actor_scd_type).is_active,
                (records::actor_scd_type).start_date,
                (records::actor_scd_type).end_date
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.actor,
                ts.quality_class,
                ts.is_active,
                ts.current_year AS start_date,
                ts.current_year AS end_date
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.actor = ls.actor
         WHERE ls.actor IS NULL

     )


SELECT *, 2021 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a