#hadoop fs -rmr /user/aman/preParserInput
#hadoop fs -rmr /user/aman/preParserOutput
#hadoop fs -copyFromLocal preParserInput preParserInput
hadoop jar preparseprocessor.jar hadoop.PreParseProcessor -libjars sfulib/stanford-corenlp-1.3.5.jar,/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,sfulib/stanford-corenlp-1.3.5-models.jar,sfulib/joda-time.jar nw_fm_preparseinput nw_fm_preparseoutput 100 40
