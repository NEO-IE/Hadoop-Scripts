#hadoop fs -rmr nw_fm_postparseroutput
hadoop fs -rmr extract_out
#hadoop fs -copyFromLocal parsed2.txt parsed.txt
hadoop jar extractor.jar LowQualityExtractionPruner -libjars /mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar sg extract_out 32
