#hadoop fs -rmr nw_fm_postparseroutput
#hadoop fs -rm parsed.txt
#hadoop fs -copyFromLocal parsed2.txt parsed.txt
INPUT=$1
OUTPUT=$2
MAPPER=$3
hadoop jar postparseprocessor.jar hadoop.PostParseProcessor -libjars sfulib/stanford-corenlp-1.3.5.jar,/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,sfulib/stanford-corenlp-1.3.5-models.jar,sfulib/joda-time.jar $INPUT $OUTPUT $MAPPER
