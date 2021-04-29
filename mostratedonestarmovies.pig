-- Load the file u.data
ratings = LOAD '/usr/local/hadoop/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);
-- Load the file u.item
metadata = LOAD '/usr/local/hadoop/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);
-- Extract useful column from the raw data
nameLookup = foreach metadata generate movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseTime;
-- Group movieID by ratings
ratingsByMovie = group ratings by movieID;
-- Extract useful column from the raw data
avgRatings = foreach ratingsByMovie generate group as movieID, AVG(ratings.rating) as avgRating COUNT(ratings.rating) as numRatings;
-- Remove unwanted rows from the averageRatings
badMovies = filter averageRatings by avgRating<2.0;
-- Join badmovies from movieID and nameLookup from movieID
namedBadMovies = join badMovies by movieID, nameLookup by movieID;
-- Extract useful columns from the raw data
finalResults = foreach namedBadMovies generate namelookup::movieTitle as movieName, badMovies::avgRating as avgRating, badMovies::numRatings as numRatings;
-- Sorting the finalResults in Descending order
finalResultsSorted = order finalResults by numRatings DESC;
-- Print the finalResultsSorted data
dump finalResultsSorted;
