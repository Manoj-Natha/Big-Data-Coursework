Part 1: (URLToHDFS.java)

Execution: hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.URLToHDFS

This program downloads the URLs and place them in HDFS. After placing them in HDFS, the text files are uncompressed and the Zip files are deleted.

The files are named by removing bz2 in the file name.

Part 2: (twitterHashtagAnalysis)

Execution: hadoop jar JavaHDFS-0.0.1-SNAPSHOT.jar JavaHDFS.JavaHDFS.twitterHashtagAnalysis

Please add the following line into Hadoop class path for adding json-simple-1.1 jar file before executing.

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/home/012/m/mx/mxn151930/json-simple-1.1.jar

This program downloads 100 tweets per day for the past 6 days into separate files named tweets-date.txt.
