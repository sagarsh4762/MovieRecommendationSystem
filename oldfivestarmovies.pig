ratings = LOAD '/usr/local/hadoop/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/usr/local/hadoop/ml-100k/u.item' USING PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRelease:chararray, imdbLink:chararray);

dump metadata;

nameLookup = foreach metadata generate movieID, movieTitle, ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseTime;

ratingsByMovie = group ratings by movieID;

dump ratingsByMovie;

avgRatings = foreach ratingsByMovie generate group as movieID, AVG(ratings.rating) as avgRating;

dump avgRatings;

describe ratings;
describe ratingsByMovie;
describe avgRatings;

fiveStarMovies = filter avgRatings by avgRating > 4.0;

describe fiveStarMovies;

describe nameLookup;

fiveStarsWithData = join fiveStarMovies by movieID, nameLookup by movieID;

describe fiveStarsWithData;

dump fiveStarsWithData;

oldestFiveStarMovies = order fiveStarsWithData by nameLookup::releaseTime;

dump oldestFiveStarMovies;