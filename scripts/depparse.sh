#This script can be used to obtain dependency parse of a set of sentences whose name is supplied as $1
parsedFile=$1parsed
echo "Transfering the file to hdfs (1/8)"
hadoop fs -copyFromLocal $1 $1

echo "Running the parser (2/8)"
hadoop jar /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/contrib/streaming/hadoop-streaming-0.20.2-cdh3u6.jar -D mapred.reduce.tasks=0 -D mapred.map.tasks=1000 -D mapred.task.timeout=0 -archives 'hdfs://b100:20080/user/aman/bllip-parser.jar' -input $1 -output $parsedFile -file  /mnt/a99/d0/aman/sgmtp/hadoopmultir/parsing/cjmapper.py  -mapper "/mnt/a99/d0/aman/sgmtp/hadoopmultir/parsing/cjmapper.py"

echo "Getting the parser results (3/8)"
hadoop fs -getmerge $parsedFile $parsedFile

echo "Removing parser results (4/8)"
hadoop fs -rmr $parsedFile

echo "Transfer merged files back (5/8)"
hadoop fs -copyFromLocal $parsedFile $parsedFile

depParseFile=$parsedFile"Dep"
echo "Running post parse prorcessor (6/8)"
hadoop jar /mnt/a99/d0/aman/sgmtp/postparseprocessor.jar hadoop.PostParseProcessor -libjars /mnt/a99/d0/aman/sgmtp/sfulib/stanford-corenlp-1.3.5.jar,/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,/mnt/a99/d0/aman/sgmtp/sfulib/stanford-corenlp-1.3.5-models.jar,/mnt/a99/d0/aman/sgmtp/sfulib/joda-time.jar $parsedFile $depParseFile 1000


echo "Getting results (7/8)"
hadoop fs -getmerge $depParseFile $2

echo "Final cleaning up  (Done)"
hadoop fs -rmr $depParseFile
hadoop fs -rm $parsedFile
