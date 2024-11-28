--- DDL for tracking cumulative data of user activity by user_id and browser_type

CREATE TABLE users_browser_cumulated (
    user_id TEXT, -- Unique identifier for the user
    browser_type TEXT, -- Type of browser used by the user (e.g., Chrome, Firefox)
    dates_active DATE[], -- Array of past dates when the user was active with this browser type
    date DATE, -- Current date being recorded for this user and browser type
    PRIMARY KEY (user_id, browser_type, date) -- Ensures uniqueness across user_id, browser_type, and the recorded date
);