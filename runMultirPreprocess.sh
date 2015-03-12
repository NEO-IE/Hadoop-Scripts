DOCFILE=$1
PPOUT=$2
MAPPER=$3
REDUCER=$4
HOMEPRE=/mnt/a99/d0/aman/sgmtp
hadoop jar multirpre.jar main.iitb.MultirPreprocessor -libjars /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,$HOMEPRE/sfulib/stanford-corenlp-1.3.5.jar,$HOMEPRE/sfulib/stanford-corenlp-1.3.5-models.jar,$HOMEPRE/sfulib/joda-time.jar $DOCFILE $PPOUT $MAPPER $REDUCER
