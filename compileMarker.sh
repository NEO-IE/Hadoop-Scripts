rm classFolder/*
javac -classpath /mnt/a99/d0/aman/sgmtp/opennlplib/commons-io-1.3.2.jar:/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar:/mnt/a99/d0/aman/sgmtp/opennlplib/common-scala_2.10-1.1.2.jar:/mnt/a99/d0/aman/sgmtp/opennlplib/org.scala-lang.scala-actors_2.11.1.v20140519-130118-1e1defd99c.jar:/mnt/a99/d0/aman/sgmtp/opennlplib/commons-lang-2.6.jar:/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar -d classFolder Hadoop_Marker/src/marker/*.java 
jar -cvf marker.jar -C classFolder/ .


