CREATE DATABASE IF NOT EXISTS GIRAFFE_IMDb;
USE GIRAFFE_IMDb;


CREATE SCHEMA IF NOT EXISTS GIRAFFE_IMDb.STAGING;
USE SCHEMA GIRAFFE_IMDb.STAGING;

CREATE OR REPLACE STAGE GIRAFFE_IMDb_STAGE;

CREATE OR REPLACE TABLE imdb_dataset (
    Title VARCHAR,
    IMDb_rating FLOAT,
    Year INT,
    Certificates VARCHAR,
    Genre VARCHAR,
    Director VARCHAR,
    Star_Cast VARCHAR,
    MetaScore INT,
    Duration_minutes INT
);


CREATE OR REPLACE TABLE imdb_dataset_2 (
    Title VARCHAR,
    IMDb_rating FLOAT,
    Year INT,
    Certificates VARCHAR,
    Genre VARCHAR,
    Director VARCHAR,
    Star_Cast VARCHAR,
    MetaScore INT,
    Poster_src VARCHAR,
    Duration_minutes INT
);

CREATE OR REPLACE TABLE imdb_dataset_3 (
    Title VARCHAR,
    Director VARCHAR,
    Star_Cast VARCHAR,
    Year INT,
    IMDb_Rating FLOAT,
    MetaScore INT,
    Certificates VARCHAR,
    Genre VARCHAR,
    Second_Genre VARCHAR,
    Third_Genre VARCHAR,
    Poster_src VARCHAR,
    Duration_minutes INT
);


CREATE OR REPLACE TABLE imdb_dataset_4 (
    Film_Name VARCHAR,
    Lead_Actor VARCHAR,
    Lead_Actress VARCHAR,
    Supporting_Actor VARCHAR,
    Supporting_Actress VARCHAR
);


CREATE OR REPLACE TABLE imdb_dataset_5 (
    Name VARCHAR,
    Duration_Minutes INT,
    Country VARCHAR,
    Language VARCHAR,
    Budget_Million FLOAT,
    Box_Office_Million FLOAT
);

COPY INTO imdb_dataset 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
COPY INTO imdb_dataset_2 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset_2.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
COPY INTO imdb_dataset_3 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset_3.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
COPY INTO imdb_dataset_4 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset_4.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
COPY INTO imdb_dataset_5 
FROM @GIRAFFE_IMDb_STAGE/IMDb_Dataset_5.csv 
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);


CREATE OR REPLACE TABLE Genre_Dim AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY Genre) AS Genre_ID,
    Genre,
    Second_Genre
FROM imdb_dataset_3;


CREATE OR REPLACE TABLE Country_Dim AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY Country) AS Country_ID,
    Country
FROM imdb_dataset_5;


CREATE OR REPLACE TABLE Language_Dim AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY Language) AS Language_ID,
    Language
FROM imdb_dataset_5;


CREATE OR REPLACE TABLE Film_Fact AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY d5.Name) AS Film_ID,
    d5.Budget_Million,
    d5.Box_Office_Million,
    d3.IMDb_Rating,
    d3.MetaScore,
    d5.Duration_Minutes,
    g.Genre_ID,
    c.Country_ID,
    l.Language_ID
FROM imdb_dataset_5 d5
LEFT JOIN imdb_dataset_3 d3 ON d5.Name = d3.Title
LEFT JOIN Genre_Dim g ON d3.Genre = g.Genre
LEFT JOIN Country_Dim c ON d5.Country = c.Country
LEFT JOIN Language_Dim l ON d5.Language = l.Language;