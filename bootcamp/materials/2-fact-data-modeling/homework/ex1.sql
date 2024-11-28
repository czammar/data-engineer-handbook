/**
    A query to deduplicate `game_details` from Day 1 so there are no duplicates.
**/

WITH

-- Selects all game IDs for the earliest game date in the dataset
games_date_1 AS (
    SELECT
        game_id
    FROM games
    WHERE 1=1
        -- Filters for the minimum date in the `game_date_est` column
        AND game_date_est = (SELECT MIN(DATE(game_date_est)) FROM games)
),

-- Adds a row number to each game detail, partitioned by game, team, and player IDs
step AS (
    SELECT 
        gd.*,
        -- Assigns a row number to each record within a group defined by game_id, team_id, and player_id
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, team_id, player_id
        ) AS row_num
    FROM game_details gd 
    -- Joins the game details with the filtered game IDs for the earliest date
    INNER JOIN games_date_1 g 
    ON g.game_id = gd.game_id
)

-- Selects the deduplicated game details for the first day
SELECT
    game_id,
    team_id,
    team_abbreviation,
    team_city,
    player_id,
    player_name,
    nickname,
    start_position,
    "comment",
    "min",
    fgm,
    fga,
    fg_pct,
    fg3m,
    fg3a,
    fg3_pct,
    ftm,
    fta,
    ft_pct,
    oreb,
    dreb,
    reb,
    ast,
    stl,
    blk 
    ""TO"",
    pf,
    pts,
    plus_minus
FROM step
-- Keeps only the first row for each game, team, and player combination
WHERE row_num = 1;
