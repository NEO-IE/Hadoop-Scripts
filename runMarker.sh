MARKER_IN=$1
MARKER_OUT=$2
MAPPER=$3
hadoop jar marker.jar marker.HadoopCountryMarker -libjars /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,/mnt/a99/d0/aman/sgmtp/opennlplib/commons-lang-2.6.jar $MARKER_IN $MARKER_OUT $MAPPER
