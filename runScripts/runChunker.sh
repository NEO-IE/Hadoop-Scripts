#hadoop fs -rm chunkerSampleInput
#hadoop fs -rmr chunkerOutput
#hadoop fs -copyFromLocal chunkerSampleInput chunkerSampleInput
CHUNKIN=$1
CHUNKOUT=$2
MAPPER=$3
hadoop jar chunker.jar hadoop.HadoopChunker -libjars sfulib/stanford-corenlp-1.3.5.jar,/mnt/b100/d0/hadoop/hadoop-0.20.2-cdh3u6/hadoop-core-0.20.2-cdh3u6.jar,sfulib/stanford-corenlp-1.3.5-models.jar,sfulib/joda-time.jar,opennlplib/common-scala_2.10-1.1.2.jar,opennlplib/commons-io-1.3.2.jar,opennlplib/commons-lang-2.6.jar,opennlplib/nlptools-chunk-opennlp_2.10-2.4.4.jar,opennlplib/nlptools-core_2.10-2.4.4.jar,opennlplib/nlptools-postag-opennlp_2.10-2.4.4.jar,opennlplib/nlptools-stem-morpha_2.10-2.4.4.jar,opennlplib/nlptools-tokenize-opennlp_2.10-2.4.4.jar,opennlplib/nlptools-wordnet-uw_2.10-2.4.4.jar,opennlplib/opennlp-chunk-models-1.5.jar,opennlplib/opennlp-postag-models-1.5.jar,opennlplib/opennlp-sent-models-1.5.jar,opennlplib/opennlp-tokenize-models-1.5.jar,opennlplib/openregex-1.1.1.jar,opennlplib/openregex-scala_2.10-1.1.2.jar,opennlplib/org.scala-lang.scala-actors_2.11.1.v20140519-130118-1e1defd99c.jar,opennlplib/org.scala-lang.scala-library_2.11.1.v20140519-130118-1e1defd99c.jar,opennlplib/org.scala-lang.scala-reflect_2.11.1.v20140519-130118-1e1defd99c.jar,opennlplib/taggers-core_2.10-0.4.jar,olp/opennlp-tools-1.5.3.jar,olp/opennlp-maxent-3.0.3.jar $CHUNKIN $CHUNKOUT $MAPPER
