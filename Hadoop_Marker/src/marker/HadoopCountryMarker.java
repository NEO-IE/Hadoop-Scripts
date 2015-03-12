package marker;

/**
 * This class is used to distribute the task of Marking countries in a sentence across blade
 * @author ashish
 *
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HadoopCountryMarker implements Tool {

	private Configuration conf;
	/**
	 * We maintain 3 forms of every country : a) India, Indi and Ind which has
	 * no space in its name. Some of the variations like US, USA were hardcoded
	 * in the file Then we iterate over words in the sentence and see if they
	 * match one of 3 forms of any of the countries
	 */
	static HashMap<String, String> freeBaseMapping;
	static HashSet<String> countryList;
	HashSet<String> popularAbbrSet;

	static {
		String countriesFile = "hdfs://b100:20080/user/aman/countries_file";
		Path pt = new Path(countriesFile);
		FileSystem fs;
		try {
			fs = FileSystem.get(new Configuration());

			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String countryRecord = null;
			freeBaseMapping = new HashMap<String, String>();
			while ((countryRecord = br.readLine()) != null) {
				String vars[] = countryRecord.split("\t");
				String countryName = vars[1].toLowerCase();
				String countryId = vars[0];
				// System.out.println(countryName);
				freeBaseMapping.put(countryName, countryId);
			}
			countryList = new HashSet<String>(freeBaseMapping.keySet());
			// System.out.println(countryList);
			// "US/USA gets special treatment as always
			br.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HadoopCountryMarker() throws IOException {
		conf = new Configuration();
		setConf(conf);

	}

	/*
	 * private static final class SentGlobalID implements
	 * CoreAnnotation<Integer>{
	 * 
	 * @Override public Class<Integer> getType() { return Integer.class; } }
	 */

	public static class Map extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text sentId, Text value,
				OutputCollector<Text, Text> collector, Reporter arg3)
				throws IOException {
			// System.out.println(value.toString());
			boolean hasCountry = false;
			boolean hasNumber = false;
			Pattern p = Pattern.compile("-?\\d+");

			ArrayList<Marking> markings = new ArrayList<Marking>();

			String tokenString = value.toString();

			String[] tokens = tokenString.split("\\s+");
			System.out.println(tokens);
			Matcher m = null;
			for (int i = 0; i < tokens.length; i++) {
				m = p.matcher(tokens[i]);
				if (m.find()) {
					hasNumber = true;
					markings.add(new Marking(i, i + 1, m.group(), m.group(), 1,
							Marking.NUMBER));
				} else if (countryList.contains(tokens[i].toLowerCase())) { // test for
																// country
					hasCountry = true;
					String freeBaseId = freeBaseMapping.get(tokens[i]
							.toLowerCase());
					// TODO : Do we need original country name?
					markings.add(new Marking(i, i + 1, WordUtils
							.capitalize(tokens[i]), freeBaseId, 1,
							Marking.COUNTRY));
				}
			}

			if (hasCountry && hasNumber) {
				ArrayList<String> markingStrings = MarkingUtils
						.getMarkingStrings(markings);

				collector.collect(
						sentId,
						new Text(markingStrings.get(MarkingUtils.LINKOFFSET)
								+ "\t"
								+ markingStrings.get(MarkingUtils.TYPEOFFSET)));
			} else {
	            collector.collect(
						sentId,
						new Text("" + "\t" + ""));

            }
			// System.out.println(docName + "\t" + key.toString() + "\t" +
			// tokenString);
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

		job.setMemoryForReduceTask(8144);
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
