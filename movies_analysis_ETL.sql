--Create HIVE database for movie analytics
CREATE DATABASE movie_analytics LOCATION '/data/analytics/raw';

--Creating raw table to store source data daily partitioned
DROP TABLE movie_analytics.credits_movies_raw;

CREATE EXTERNAL TABLE movie_analytics.credits_movies_raw
(
movie_cast STRING,
movie_crew STRING,
movies_id INT
)
PARTITIONED BY (load_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  "separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\"
  )
STORED AS TEXTFILE
LOCATION '/data/analytics/raw/credits_movies_raw'
TBLPROPERTIES("skip.header.line.count"="1");


--Load data
LOAD DATA INPATH '/data/analytics/raw/files'
OVERWRITE INTO TABLE movie_analytics.credits_movies_raw PARTITION (load_date='2021-12-08');

--Creating derived tables for cast and crew for analytics
--Cast derived table

DROP TABLE movie_analytics.credits_movies_cast;

CREATE EXTERNAL TABLE movie_analytics.credits_movies_cast
(
movies_id int,
cast_id int,
cast_character string,
cast_credit_id string,
cast_gender int,
movie_cast_id int,
cast_name string,
cast_order int,
cast_profile_path string
)
PARTITIONED BY (load_date string)
STORED AS ORC
LOCATION '/data/analytics/final/credits_movies_cast';

--Insert query to load data from raw table cast table
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE movie_analytics.credits_movies_cast PARTITION(load_date)
SELECT
movies_id,
movie_cast['cast_id'] as cast_id,
movie_cast['character'] as cast_character,
movie_cast['credit_id'] as cast_credit_id,
movie_cast['gender'] as cast_gender,
movie_cast['id'] as movie_cast_id,
movie_cast['name'] as cast_name,
movie_cast['order'] as cast_order,
movie_cast['profile_path'] as cast_profile_path,
load_date
FROM
(
SELECT
movies_id,
str_to_map(replace(translate(mv_cast_map,'{[}]\'',''),', ',','),',',':') as movie_cast,
load_date
FROM
(
SELECT
movies_id,
mv_cast,
load_date
FROM movie_analytics.credits_movies_raw stg
LATERAL VIEW EXPLODE(SPLIT(SUBSTR(stg.movie_cast,2),'(?<=\\}]),(?=\\{)')) ext as mv_cast
--WHERE load_date='${hivevar:load_date}'
) stg1
LATERAL VIEW EXPLODE(SPLIT(mv_cast,'}, ')) ext as mv_cast_map
) stg2;

--Creating derived tables for cast and crew for analytics
--crew derived table

DROP TABLE movie_analytics.credits_movies_crew;

CREATE EXTERNAL TABLE movie_analytics.credits_movies_crew
(
movies_id int,
crew_credit_id int,
crew_department string,
crew_gender int,
movie_crew_id int,
crew_job string,
crew_name string,
crew_profile_path string
)
PARTITIONED BY (load_date string)
STORED AS ORC
LOCATION '/data/analytics/final/credits_movies_crew';

--Insert query to load data from raw table cast table
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE movie_analytics.credits_movies_crew PARTITION(load_date)
SELECT
movies_id,
movie_crew['credit_id'] as crew_credit_id,
movie_crew['department'] as crew_department,
movie_crew['gender'] as crew_gender,
movie_crew['id'] as movie_crew_id,
movie_crew['job'] as carew_name,
movie_crew['name'] as carew_name,
movie_crew['profile_path'] as crew_profile_path,
load_date
FROM
(
SELECT
movies_id,
str_to_map(replace(translate(mv_crew_map,'{[}]\'',''),', ',','),',',':') as movie_crew,
load_date
FROM
(
SELECT
movies_id,
mv_crew,
load_date
FROM movie_analytics.credits_movies_raw stg
LATERAL VIEW EXPLODE(SPLIT(SUBSTR(stg.movie_crew,2),'(?<=\\}]),(?=\\{)')) ext as mv_crew
--WHERE load_date='${hivevar:load_date}'
) stg1
LATERAL VIEW EXPLODE(SPLIT(mv_crew,'}, ')) ext as mv_crew_map
) stg2;


--View Creation
DROP VIEW IF EXISTS movie_analytics.credits_movies_cast_vw;

CREATE VIEW movie_analytics.credits_movies_cast_vw
SELECT
movies_id,
cast_credit_id,
cast_department,
cast_gender, 
movie_crew_id,
cast_job,
cast_name,
cast_profile_path, 
load_date
FROM movie_analytics.credits_movies_cast;

DROP VIEW IF EXISTS movie_analytics.credits_movies_crew_vw;

CREATE VIEW movie_analytics.credits_movies_crew_vw
SELECT
movies_id,
crew_credit_id,
crew_department,
crew_gender, 
movie_crew_id,
crew_job,
crew_name,
crew_profile_path, 
load_date
FROM movie_analytics.credits_movies_crew;


