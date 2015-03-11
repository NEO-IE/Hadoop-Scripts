javac -source 1.6 -target 1.6 -classpath sfulib/*:/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar:. -d classFolder hadoopmultir/post-parse-processing/src/hadoop/PostParseProcessor.java
jar -cvf postparseprocessor.jar -C classFolder/ .
