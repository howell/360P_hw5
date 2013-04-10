rm -rf output/
#javac -classpath /usr/local/Cellar/hadoop/1.1.2/libexec/hadoop-core-1.1.2.jar -d gen/ TextAnalyzer.java 
javac -classpath /Users/hadoop/hadoop-1.0.4/hadoop-core-1.0.4.jar -d gen/ TextAnalyzer.java
jar -cvf TextAnalyzer.jar -C gen/ .
/Users/hadoop/hadoop-1.0.4/bin/hadoop jar TextAnalyzer.jar TextAnalyzer input/ output/ $1 $2
