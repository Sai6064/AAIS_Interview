
--Writing Analytics Queries for data analysis

--1. Top 10 Actors 

SELECT 
cast_name,
COUNT(movies_id) as movies_acted
FROM movie_analytics.credits_movies_cast
GROUP BY 
cast_name
SORT BY movies_acted DESC
LIMIT 10;

--2. Top 10 Directors 

SELECT 
crew_name,
COUNT(movies_id) as movies_directed
FROM movie_analytics.credits_movies_crew
WHERE crew_job='Director'
GROUP BY 
movies_id
SORT BY movies_acted DESC
LIMIT 10;

--3.  Gender ratio in each movie
SELECT 
movies_id,
(COUNT(CASE WHEN cast_gender='1' then 'Male' END)/COUNT(cast_id))*100 AS male_cast_percentage,
(COUNT(CASE WHEN cast_gender='2' then 'Female' END)/COUNT(cast_id))*100 AS female_cast_percentage
FROM movie_analytics.credits_movies_cast
GROUP BY 
movies_id


--4. Number of Cast and crew in each movie
with 
cast_mv as
(
SELECT 
movies_id,
COUNT(cast_id) as total_cast
FROM movie_analytics.credits_movies_cast
GROUP BY 
movies_id
),
crew_mv as
(SELECT 
movies_id,
COUNT(movie_crew_id) as total_crew
FROM movie_analytics.credits_movies_crew
GROUP BY 
movies_id)
select
a.movies_id,
a.total_cast,
b.total_crew
from cast_mv a inner join crew_mv b on (a.movies_id = b.movies_id);

--4. Distinct Characters in each movie
SELECT 
movies_id,
COUNT(cast_character) as total_characters
FROM movie_analytics.credits_movies_cast
GROUP BY 
movies_id;