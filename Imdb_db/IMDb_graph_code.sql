SELECT 
    Title,
    AVG(IMDb_rating) AS Average_Rating
FROM imdb_dataset
GROUP BY Title
ORDER BY Average_Rating DESC
LIMIT 5;


SELECT 
    DISTINCT Title,
    Duration_minutes
FROM imdb_dataset
ORDER BY Duration_minutes DESC;

SELECT 
    Country,
    COUNT(*) AS Film_Count
FROM imdb_dataset_5
GROUP BY Country
ORDER BY Film_Count DESC;

SELECT 
    DISTINCT Title,
    IMDb_rating
FROM imdb_dataset
WHERE Star_Cast LIKE '%Leonardo DiCaprio%'
ORDER BY IMDb_rating DESC;


SELECT 
    DISTINCT Title,
    Genre
FROM imdb_dataset
WHERE Genre LIKE '%Fantasy%'
ORDER BY Title;