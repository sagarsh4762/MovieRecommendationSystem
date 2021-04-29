-- Load the file u.data
ratings = LOAD '/usr/local/hadoop/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
-- Load the file u.item
metadata = LOAD '/usr/local/hadoop/ml-100k/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
-- Print the metadata
dump metadata;
-- Extract useful column from the raw data
nameLookup = foreach metadata generate movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseTime;
-- Group movieID by ratings
ratingsByMovie = group ratings by movieID;
-- Print the ratingsByMovie
dump ratingsByMovie;
-- Extract useful column from the raw data
avgRatings = foreach ratingsByMovie generate group as movieID, AVG(ratings.rating) as avgRating;
-- Print the avgRatings
dump avgRatings;
-- Detail schema of the ratings
describe ratings;
-- Detail schema of the ratingsByMovie
describe ratingsByMovie;
-- Detail schema of the avgRatings
describe avgRatings;
-- Remove unwanted rows from avgRatings
fiveStarMovies = filter avgRatings by avgRating > 4.0;
-- Detail schema of the fiveStarMovies
describe fiveStarMovies;
-- Detail schema of the nameLookup
describe nameLookup;
-- Join movieID with fiveStarMovies and movieID with nameLookup
fiveStarsWithData = join fiveStarMovies by movieID, nameLookup by movieID;
-- Detail schema of the fiveStarWithData
describe fiveStarsWithData;
-- Print the fiveStarWithData 
dump fiveStarsWithData;
-- Sorting the fiveStarWithData with respect to releasetime
oldestFiveStarMovies = order fiveStarsWithData by nameLookup::releaseTime;
-- Print the oldestFiveStarMovies
dump oldestFiveStarMovies;
