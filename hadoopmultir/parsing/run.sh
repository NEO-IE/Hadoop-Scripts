#!/bin/bash
IP=$1
OP=$2
NUM_MAP=$3
hadoop jar /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/contrib/streaming/hadoop-streaming-0.20.2-cdh3u6.jar -D mapred.reduce.tasks=0 -D mapred.map.tasks=$NUM_MAP -D mapred.task.timeout=3600000 -archives 'hdfs://b100:20080/user/aman/bllip-parser.jar' -input  $IP -output $OP -file /mnt/a99/d0/aman/sgmtp/hadoopmultir/parsing/cjmapper.py -mapper "python cjmapper.py"
#hadoop jar /path/to/hadoop-streaming-1.0.2.jar -D mapred.reduce.tasks=0 -D mapred.map.tasks=x -D mapred.task.timeout=LONGTIMEOUT -archives /hdfs/path/to/bllip-parser.jar' -input /hdfs/path/to/tokenized/sentences -output /hdfs/path/to/parsed/output -file cjmapper.py -mapper "python cjmapper.py"
