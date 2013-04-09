rm -rf output/
javac -classpath /usr/local/Cellar/hadoop/1.1.2/libexec/hadoop-core-1.1.2.jar -d gen/ TextAnalyzer.java 
jar -cvf TextAnalyzer.jar -C gen/ .
hadoop jar TextAnalyzer.jar TextAnalyzer input/ output/ $1 $2
