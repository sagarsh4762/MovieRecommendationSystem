# MovieRecommendationSystem

In this post, we'll tackle one of the challenges of learning Hadoop, and that's finding data sets that are realistic yet large enough to show the advantages of distributed processing, but small enough for a single developer to tackle. The data set we're using in this tutorial is movielens movie data. The data is available by year, and includes detailed descriptions of movies, ratings and users. This data is especially well-suited for our purposes, because a great deal of it is hand-encoded, so there are errors and malformed records that we need to handle.

Each year contains several types of files:Ratings file, users file,movies file.

RATINGS FILE DESCRIPTION
================================================================================

All ratings are contained in the file "ratings.dat" and are in the
following format:

UserID::MovieID::Rating::Timestamp

- UserIDs range between 1 and 6040 
- MovieIDs range between 1 and 3952
- Ratings are made on a 5-star scale (whole-star ratings only)
- Timestamp is represented in seconds since the epoch as returned by time(2)
- Each user has at least 20 ratings

USERS FILE DESCRIPTION
================================================================================

User information is in the file "users.dat" and is in the following
format:

UserID::Gender::Age::Occupation::Zip-code

All demographic information is provided voluntarily by the users and is
not checked for accuracy.  Only users who have provided some demographic
information are included in this data set.

- Gender is denoted by a "M" for male and "F" for female
- Age is chosen from the following ranges:

	*  1:  "Under 18"
	* 18:  "18-24"
	* 25:  "25-34"
	* 35:  "35-44"
	* 45:  "45-49"
	* 50:  "50-55"
	* 56:  "56+"

- Occupation is chosen from the following choices:

	*  0:  "other" or not specified
	*  1:  "academic/educator"
	*  2:  "artist"
	*  3:  "clerical/admin"
	*  4:  "college/grad student"
	*  5:  "customer service"
	*  6:  "doctor/health care"
	*  7:  "executive/managerial"
	*  8:  "farmer"
	*  9:  "homemaker"
	* 10:  "K-12 student"
	* 11:  "lawyer"
	* 12:  "programmer"
	* 13:  "retired"
	* 14:  "sales/marketing"
	* 15:  "scientist"
	* 16:  "self-employed"
	* 17:  "technician/engineer"
	* 18:  "tradesman/craftsman"
	* 19:  "unemployed"
	* 20:  "writer"

MOVIES FILE DESCRIPTION
================================================================================

Movie information is in the file "movies.dat" and is in the following
format:

MovieID::Title::Genres

- Titles are identical to titles provided by the IMDB (including
year of release)
- Genres are pipe-separated and are selected from the following genres:

	* Action
	* Adventure
	* Animation
	* Children's
	* Comedy
	* Crime
	* Documentary
	* Drama
	* Fantasy
	* Film-Noir
	* Horror
	* Musical
	* Mystery
	* Romance
	* Sci-Fi
	* Thriller
	* War
	* Western

- Some MovieIDs do not correspond to a movie due to accidental duplicate
entries and/or test entries
- Movies are mostly entered by hand, so errors and inconsistencies may exist

The data is available in gamedata directory.

Since we have comma-delimited, newline-terminated records, we can use Pig’s built-in PigStorage class to get some more in-depth information about our data set. Let’s start with a few basic questions:

How many old five star movies?

How many most rated one star movies?

From the given data set, we know that each game record starts with an id line like the one shown above. We can write a simple pig script to filter out those records and count them:

For solving the above given problem I am going to use pig script. Whole pig script query have been written caring about HDFS datastoreage. So for running the pig script query firstly you have to store the whole data on HDFS.

Store Data On HDFS

$ hadoop fs -mkdir /gamedata
$ hadoop fs -copyFromLocal /home/maniram/data/* /gamedata

Now you can count the number of match played by running pig script file

Command to run total_counts.pig script file

  
  $ pig -x mapreduce '/home/maniram/total_counts.pig'
  
  -- replace maniram with user name of your OS in above line
  
Now you can extraxt information of each and player and you can find inter relation between them using following command

Command to run id_to_name.pig script file

  
  $ pig -x mapreduce '/home/maniram/id_to_name.pig'
  -- replace maniram with user name of your OS in above line
  
Now all the data have been saved in HDFS. You can acess these data by using hive in the tabular format. And by this way you can find relation between id and players name.

For linking player_id file of HDFS to your hive you have to create External Hive table by giving location to these file in HDFS. You have to execute this query in hive shell

$ hive

hive> REATE EXTERNAL TABLE players(id STRING, name string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n' LOCATION '/gamedata/id_player_mapping/';

-- you can maintaing LOCATION value according to your HDFS file location in above script.

Now run the following hive query which will show whole relation between id and player.


hive> select * from players;
So finally all the tree question mentioned above on the top of this file have been answered.
