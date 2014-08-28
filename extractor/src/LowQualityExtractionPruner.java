import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

//sg
/**
 * This class takes the extractions given by multir and deletes the ones that
 * have score less than a specified score
 * 
 * @author aman
 * 
 */
public class LowQualityExtractionPruner implements Tool {

	public static final int THRESHOLD = 1500000;
	public static final int SCORE_INDEX = 8;
	Configuration conf;

	public LowQualityExtractionPruner() {
		conf = new Configuration();
		setConf(conf);
	}
	
	public static class ExtractionMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, LongWritable, Text> {

		@Override
		public void map(LongWritable key, Text line,
				OutputCollector<LongWritable, Text> collector, Reporter arg3)
				throws IOException {
			String lineStr = line.toString();
			String vals[] = lineStr.split("\t");
			Integer score = Integer.parseInt(vals[SCORE_INDEX]);
			if (score > THRESHOLD) {
				collector.collect(key, line);
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
		if (args.length != 2) {
			System.err.println("Usage : <input> <score> <output>");
			return -1;
		}
		JobConf job = new JobConf(getConf(), LowQualityExtractionPruner.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);

		job.setJobName("extraction-pruner");
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setMapperClass(ExtractionMapper.class);

		job.setMemoryForReduceTask(8144);
		job.set("mapred.child.java.opts", "-Xmx6g");
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		job.setNumMapTasks(Integer.parseInt(args[2]));
		job.setNumReduceTasks(0);

		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new LowQualityExtractionPruner(), args);
		
	}
}
