"""
Exercise 1. Loading data recursively to actors
"""

import psycopg2
import time

for i in range(1969,2021):
    connection = psycopg2.connect(user="postgres",
                                    password="postgres",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="postgres")
    
    cursor = connection.cursor()
        
    QUERY= f"""
    INSERT INTO actors
    WITH
    last_year AS (
            SELECT
                actor,
                actorid,
                YEAR,
                ARRAY_AGG(films_data) AS films_data_array,
                AVG(rating) AS avg_rating
            FROM (
            SELECT
                actor,
                actorid,
                YEAR,
                rating,
                ROW(
                    film,
                    votes,
                    rating,
                    filmid,
                    year)::film_data AS films_data
            FROM public.actor_films af
            WHERE 1=1
                AND af.YEAR = {i}
            ) step1
            GROUP BY actor, actorid, year
            ),

            this_year AS (
            SELECT
                actor,
                actorid,
                YEAR,
                ARRAY_AGG(films_data) AS films_data_array,
                AVG(rating) AS avg_rating
            FROM (
            SELECT
                actor,
                actorid,
                YEAR,
                rating,
                ROW(
                    film,
                    votes,
                    rating,
                    filmid,
                    year)::film_data AS films_data
            FROM public.actor_films af
            WHERE 1=1
                AND af.YEAR = {i+1}
            ) step1
            GROUP BY actor, actorid, year
            )

            SELECT 
                COALESCE(ty.actor, ly.actor) AS actor,
                COALESCE(ty.actorid, ly.actorid) AS actorid,
                COALESCE(
                    ty.films_data_array, ARRAY[]::film_data[]
                ) || 
                COALESCE(
                    ly.films_data_array, ARRAY[]::film_data[]
                ) AS films,
                COALESCE(ty.YEAR, ly.YEAR+1) AS current_year,
                (CASE
                    WHEN COALESCE(ty.avg_rating, ly.avg_rating) > 8 THEN 'star'
                    WHEN COALESCE(ty.avg_rating, ly.avg_rating) > 7 THEN 'good'
                    WHEN COALESCE(ty.avg_rating, ly.avg_rating) > 6 THEN 'average'
                    ELSE 'bad'
                END)::quality_class_rate AS quality_class,
                CASE
                    WHEN ty.YEAR IS NOT NULL THEN TRUE
                    ELSE FALSE
        END is_active
        FROM last_year ly
        FULL OUTER JOIN this_year ty
        ON ly.actorid = ty.actorid;
        """

    cursor.execute(QUERY)
    connection.commit()

    cursor.close()
    connection.close()

    time.sleep(1)



