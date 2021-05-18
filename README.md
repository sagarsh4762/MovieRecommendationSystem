# MovieRecommendationSystem

In this post, we'll tackle one of the challenges of learning Hadoop, and that's finding data sets that are realistic yet large enough to show the advantages of distributed processing, but small enough for a single developer to tackle. The data set we're using in this tutorial is movielens movie data. The data is available by year, and includes detailed descriptions of movies, ratings and users. This data is especially well-suited for our purposes, because a great deal of it is hand-encoded, so there are errors and malformed records that we need to handle.

The data is available in ml-100k directory.

Since we have comma-delimited, newline-terminated records, we can use Pig’s built-in PigStorage class to get some more in-depth information about our data set. Let’s start with a few basic questions:

How many old five star movies?

How many most rated one star movies?

From the given data set, we know that each movie record starts with an id line like the one shown above. We can write a simple pig script to filter out those records and count them:

For solving the above given problem I am going to use pig script. Whole pig script query have been written caring about HDFS datastoreage. So for running the pig script query firstly you have to store the whole data on HDFS.

Store Data On HDFS

$ hadoop fs -mkdir /ml-100k
$ hadoop fs -copyFromLocal /home/sagarsh/data/* /ml-100k

Now you can count the number of match played by running pig script file

Command to run oldfivestarmovies.pig script file

  
  $ pig -x mapreduce '/usr/local/hadoop/oldfivestarmovies.pig'
  
  -- replace the location of your OS in above line
  
Now you can extraxt information about movies.

Command to run mostratedonestarmovies.pig script file

  
  $ pig -x mapreduce '/usr/local/hadoop/mostratedonestarmovies.pig'
  -- replace the location of your OS in above line
  
