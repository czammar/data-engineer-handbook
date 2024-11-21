CREATE TYPE film_data AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT,
	YEAR INTEGER
);


CREATE TYPE quality_class_rate AS ENUM(
    'star',
    'good',
    'average',
    'bad'
);


DROP TABLE IF EXISTS actors;


CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films film_data[],
	current_year INTEGER,
	quality_class quality_class_rate,
	is_active BOOLEAN,
	PRIMARY KEY (actorid, current_year)
);