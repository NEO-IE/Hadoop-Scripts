javac -source 1.6 -target 1.6 -classpath sfulib/stanford-corenlp-1.3.5.jar:/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar:sfulib/stanford-corenlp-1.3.5-models.jar:sfulib/joda-time-2.1.jar -d classFolder hadoopmultir/pre-parse-processing/src/hadoop/PreParseProcessor.java
jar -cvf preparseprocessor.jar -C classFolder/ .
