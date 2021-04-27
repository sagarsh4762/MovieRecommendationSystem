# MovieRecommendationSystem

In this post, we'll tackle one of the challenges of learning Hadoop, and that's finding data sets that are realistic yet large enough to show the advantages of distributed processing, but small enough for a single developer to tackle. The data set we're using in this tutorial is movielens movie data. The data is available by year, and includes detailed descriptions of movies, ratings and users. This data is especially well-suited for our purposes, because a great deal of it is hand-encoded, so there are errors and malformed records that we need to handle.

SUMMARY & USAGE LICENSE
=============================================

MovieLens data sets were collected by the GroupLens Research Project
at the University of Minnesota.
 
This data set consists of:
	* 100,000 ratings (1-5) from 943 users on 1682 movies. 
	* Each user has rated at least 20 movies. 
        * Simple demographic info for the users (age, gender, occupation, zip)

The data was collected through the MovieLens web site
(movielens.umn.edu) during the seven-month period from September 19th, 
1997 through April 22nd, 1998. This data has been cleaned up - users
who had less than 20 ratings or did not have complete demographic
information were removed from this data set. Detailed descriptions of
the data file can be found at the end of this file.

Neither the University of Minnesota nor any of the researchers
involved can guarantee the correctness of the data, its suitability
for any particular purpose, or the validity of results based on the
use of the data set.  The data set may be used for any research
purposes under the following conditions:

     * The user may not state or imply any endorsement from the
       University of Minnesota or the GroupLens Research Group.

     * The user must acknowledge the use of the data set in
       publications resulting from the use of the data set
       (see below for citation information).

     * The user may not redistribute the data without separate
       permission.

     * The user may not use this information for any commercial or
       revenue-bearing purposes without first obtaining permission
       from a faculty member of the GroupLens Research Project at the
       University of Minnesota.

DETAILED DESCRIPTIONS OF DATA FILES
==============================================

Here are brief descriptions of the data.

ml-data.tar.gz   -- Compressed tar file.  To rebuild the u data files do this:
                gunzip ml-data.tar.gz
                tar xvf ml-data.tar
                mku.sh

u.data     -- The full u data set, 100000 ratings by 943 users on 1682 items.
              Each user has rated at least 20 movies.  Users and items are
              numbered consecutively from 1.  The data is randomly
              ordered. This is a tab separated list of 
	         user id | item id | rating | timestamp. 
              The time stamps are unix seconds since 1/1/1970 UTC   

u.info     -- The number of users, items, and ratings in the u data set.

u.item     -- Information about the items (movies); this is a tab separated
              list of
              movie id | movie title | release date | video release date |
              IMDb URL | unknown | Action | Adventure | Animation |
              Children's | Comedy | Crime | Documentary | Drama | Fantasy |
              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |
              Thriller | War | Western |
              The last 19 fields are the genres, a 1 indicates the movie
              is of that genre, a 0 indicates it is not; movies can be in
              several genres at once.
              The movie ids are the ones used in the u.data data set.

u.genre    -- A list of the genres.

u.user     -- Demographic information about the users; this is a tab
              separated list of
              user id | age | gender | occupation | zip code
              The user ids are the ones used in the u.data data set.

u.occupation -- A list of the occupations.

u1.base    -- The data sets u1.base and u1.test through u5.base and u5.test
u1.test       are 80%/20% splits of the u data into training and test data.
u2.base       Each of u1, ..., u5 have disjoint test sets; this if for
u2.test       5 fold cross validation (where you repeat your experiment
u3.base       with each training and test set and average the results).
u3.test       These data sets can be generated from u.data by mku.sh.
u4.base
u4.test
u5.base
u5.test

ua.base    -- The data sets ua.base, ua.test, ub.base, and ub.test
ua.test       split the u data into a training set and a test set with
ub.base       exactly 10 ratings per user in the test set.  The sets
ub.test       ua.test and ub.test are disjoint.  These data sets can
              be generated from u.data by mku.sh.

allbut.pl  -- The script that generates training and test sets where
              all but n of a users ratings are in the training data.

mku.sh     -- A shell script to generate all the u data sets from u.data.


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
