DOCFILE=$1
PPOUT=$2
MAPPER=$3
REDUCER=$4
hadoop jar marker.jar marker.HadoopCountryMarker -libjars /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,/mnt/a99/d0/aman/sgmtp/opennlplib/commons-lang-2.6.jar $DOCFILE $PPOUT $MAPPER $REDUCER
