DROP DATABASE IF EXISTS tweets;
CREATE DATABASE tweets;
USE tweets;
DROP TABLE IF EXISTS Ukraine;
CREATE TABLE Ukraine (
  event_time TIMESTAMP,
  screen_name VARCHAR(50),
  text VARCHAR(200),
  hashtags VARCHAR(50),
  coordinates VARCHAR(50),
  country VARCHAR(50),
  country_code VARCHAR(50),
  location VARCHAR(50),
  PRIMARY KEY(event_time)
);
