#!/bin/bash
#------- USE ------- #
# make sure HADOOP_PREFIX is set to the location of hadoop-core-*.jar
# the input files should be in a folder
# currently uses the file /user/hadoop/output - this can be changed
# the first argument is the name of the program to run, in this case WordCount2
# the second and third arguments are the context and querywords, so
# source run.sh WordCount2 Darcy money
# will remove the output folder, compule the WordCount2.java file, and run
# mapreduce looking for Darcy with money. The script then attempts to output the
# result, but depending on your system this may be at a different location.
rm -rf output/ gen/
hadoop dfs -rmr /user/hadoop/output
mkdir output
mkdir gen
javac -classpath $HADOOP_PREFIX/hadoop-core-*.jar -d gen/ $1.java
jar -cvf $1.jar -C gen/ .
hadoop jar $1.jar $1 input/ ./output/ $2 $3
hadoop dfs -cat /user/hadoop/output/part*
