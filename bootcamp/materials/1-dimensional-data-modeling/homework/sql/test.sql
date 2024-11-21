-- Active: 1731710968740@@127.0.0.1@5432@postgres@public
-- define a data type for main film data


WITH 

last_year AS (
SELECT
	actor,
	actorid,
	YEAR,
	ARRAY_AGG(films_data) AS films_data_array
FROM (
SELECT
	actor,
	actorid,
	YEAR,
	ROW(
		film,
		votes,
		rating,
		filmid)::film_metadata AS films_data
FROM public.actor_films af
WHERE 1=1
	AND af.YEAR = 1969
) step1
GROUP BY actor, actorid, year
),

this_year AS (
SELECT
	actor,
	actorid,
	YEAR,
	ARRAY_AGG(films_data) AS films_data_array
FROM (
SELECT
	actor,
	actorid,
	YEAR,
	ROW(
		film,
		votes,
		rating,
		filmid)::film_metadata AS films_data
FROM public.actor_films af
WHERE 1=1
	AND af.YEAR = 1970
) step1
GROUP BY actor, actorid, year
)

SELECT 
	COALESCE(ty.actor, ly.actor) AS actor,
	COALESCE(ty.actorid, ly.actorid) AS actorid,
	COALESCE(
	ty.films_data_array,ARRAY[]::film_metadata[]
	) || CASE WHEN ly.films_data_array IS NOT NULL THEN
                ly.films_data_array
                ELSE ARRAY[]::film_metadata[] END
    as films_data,
    CASE
    	WHEN ty.YEAR IS NOT NULL THEN ty.YEAR 
    	ELSE ly.YEAR+1
    END AS current_year
FROM last_year ly
FULL OUTER JOIN this_year ty
ON ly.actorid = ty.actorid;

