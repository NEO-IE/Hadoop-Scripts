//sg
/**
 * This class is used to distribute the preprocessing steps Mapper: (docname,
 * sentence) Reducer: Run the pipeline on each of the sentences
 * 
 * @author aman
 * 
 */
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import marker.HadoopCountryMarker;
import marker.HadoopCountryMarker.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;


public class MultirPreprocessor implements Tool {
	

	private Configuration conf;
	
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text docName, Text sentence, OutputCollector<Text, Text> collector, Reporter arg3)
				throws IOException {
			collector.collect(docName, sentence);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private final static Properties props = new Properties();

		static {
			props.put("annotators", "pos,lemma,ner");
			props.put("sutime.binders", "0");
		}

		private final static StanfordCoreNLP pipeline = new StanfordCoreNLP(props, false);

		@Override
		public void reduce(Text docName, Iterator<Text> sentences, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			/**
			 * collect all the sentences that are in the document
			 */
			
			String docString = "";
			int numSentences = 0;
			while (sentences.hasNext()) {
				Text sentence = sentences.next();
				docString += (sentence.toString() + "\n");
				numSentences += 1;
			}
			System.out.println("Sentences: " + numSentences);
			ArrayList<String> result = preprocess(docString);
			for(String sent: result) {
				collector.collect(docName, new Text(sent));
			}

		}

		/**
		 * Receives a document string from the reducer and returns an annotated
		 * document as an arraylist of strings.
		 * 
		 * @param fileString
		 * @throws FileNotFoundException
		 */
		public ArrayList<String> preprocess(String docString) throws FileNotFoundException {
			Annotation doc = new Annotation(docString);
			pipeline.annotate(doc);
			ArrayList<String> result = new ArrayList<String>();
			List<CoreMap> sentences = doc.get(SentencesAnnotation.class);

			int sentOffSet = 0;

			for (CoreMap sentence : sentences) {

				try {
					StringBuffer sentenceProcessed = new StringBuffer();
					List<CoreLabel> tokens = sentence.get(TokensAnnotation.class); // #3
																					// tokens
					for (CoreLabel token : tokens) {
						sentenceProcessed.append(token + " ");
					}
					sentenceProcessed.append("\t");
					for (CoreLabel token : tokens) { // #4: token offsets
						Integer sentStart = sentence.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
						Integer tokenStart = token.get(CoreAnnotations.CharacterOffsetBeginAnnotation.class);
						Integer tokenEnd = token.get(CoreAnnotations.CharacterOffsetEndAnnotation.class);
						String offsetStr = Integer.toString(tokenStart - sentStart) + ":"
								+ Integer.toString(tokenEnd - sentStart) + " ";
						sentenceProcessed.append(offsetStr + " ");
					}
					sentenceProcessed.append("\t");
					sentenceProcessed.append(sentOffSet + " ");
					sentOffSet += sentence.toString().length() + 1;
					sentenceProcessed.append(sentOffSet + "\t" + sentence + "\n");
					result.add(sentenceProcessed.toString());
				} catch (Exception e) {
					System.out.println(e);
				}
			}
			return result;

		}

	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		conf = arg0;
	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		JobConf job = new JobConf(conf, HadoopCountryMarker.class);

		// process command line options
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		job.setJobName("hadoop-country-marker");
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(Map.class);

		job.set("mapred.child.java.opts", "-Xmx6g");
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.setNumReduceTasks(0);

		JobClient.runJob(job);
		return 0;
	}

	/**
	 * 
	 * @param args
	 *            hdfsIn, hdfsOut, countriesFile, MapTask
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new HadoopCountryMarker(), args);
	}
}
