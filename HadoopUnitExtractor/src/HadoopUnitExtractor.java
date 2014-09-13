//sg
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class HadoopUnitExtractor implements Tool {
	
	Configuration conf;
	private static UnitExtractor uint;
	static {
		unit = new UnitExtractor();
	}
	public HadoopUnitExtractor() {
		conf = new Configuration();
		setConf(conf);
	}

	public static class NumberUnitExtractionMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		public void map(LongWritable key, Text line,
				OutputCollector<NullWritable, Text> collector, Reporter arg3)
				throws IOException {
			try { //getting the numbers
				collector.collect(key, u.extract(line.toString()));
			} catch() {
				
			}
		}
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		this.conf = arg0;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage : <input> <output> <maps>");
			return -1;
		}
		JobConf job = new JobConf(getConf(), HadoopUnitExtractor.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		job.setJobName("number-unit-extractor");
		job.setInputFormat(TextInputFormat.class);

		job.setMapOutputKeyClass(LongKeyWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(NumberUnitExtractionMapper.class);

		job.set("mapred.child.java.opts", "-Xmx6g");
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.setNumReduceTasks(0);

		JobClient.runJob(job);
		return 0;
	}

	public static void main(String args[]) throws Exception {
		ToolRunner.run(new HadoopUnitExtractor(), args);

	}
}
