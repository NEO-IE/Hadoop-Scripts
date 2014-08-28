javac -source 1.6 -target 1.6 -classpath /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar -d extractor/bin extractor/src/LowQualityExtractionPruner.java
jar -cvf extractor.jar -C extractor/bin .
