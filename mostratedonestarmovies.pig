ratings = LOAD '/usr/local/hadoop/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/usr/local/hadoop/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

nameLookup = foreach metadata generate movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseTime;

ratingsByMovie = group ratings by movieID;

avgRatings = foreach ratingsByMovie generate group as movieID, AVG(ratings.rating) as avgRating COUNT(ratings.rating) as numRatings;

badMovies = filter averageRatings by avgRating<2.0;

namedBadMovies = join badMovies by movieID, nameLookup by movieID;

finalResults = foreach namedBadMovies generate namelookup::movieTitle as movieName, badMovies::avgRating as avgRating, badMovies::numRatings as numRatings;

finalResultsSorted = order finalResults by numRatings DESC;

dump finalResultsSorted;