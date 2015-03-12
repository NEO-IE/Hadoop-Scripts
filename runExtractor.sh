INPUT=$1
OUTPUT=$2
MAPPER=$3
hadoop jar extractor.jar LowQualityExtractionPruner -libjars /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar $INPUT $OUTPUT $MAPPER
