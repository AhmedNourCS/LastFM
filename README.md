To start the hadoop go to sbin directory run the start-all.cmd

add hadoop-common-2.7.1.jar, hadoop-mapreduce-client-core-2.7.1 into class path

To make executable jar , follow the following steps

step 1: move file to hadoop


D:\hadoop-2.7.1\share\hadoop\mapreduce>hadoop dfs -put C:\Users\xxx\Desktop\lastfm.txt /

step 2: reading a file

D:\hadoop-2.7.1\share\hadoop\mapreduce>hadoop dfs -cat /lastfm.txt

step 3: running the examples
D:\hadoop\hadoop-2.7.1\sbin>hadoop jar D:\freelance-work\hadoopMR\LastFM\out\artifacts\LastFM_jar\LastFM.jar /lastfm.txt /LFMEx



step 4: to see the outputdir

D:\hadoop-2.7.1\share\hadoop\mapreduce>hadoop dfs -ls /LFMEx

step 5: to see the outputdir results

D:\hadoop-2.7.1\share\hadoop\mapreduce>hadoop dfs -cat /LFMEx/part-r-00000