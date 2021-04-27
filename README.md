# MovieRecommendationSystem

In this post, we'll tackle one of the challenges of learning Hadoop, and that's finding data sets that are realistic yet large enough to show the advantages of distributed processing, but small enough for a single developer to tackle. The data set we're using in this tutorial is movielens movie data. The data is available by year, and includes detailed descriptions of movies, ratings and users. This data is especially well-suited for our purposes, because a great deal of it is hand-encoded, so there are errors and malformed records that we need to handle.

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
