import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		// arguments
		if (args.length < 2) {
			System.err.printf(
					"Usage: %s [generic options] <input path> <output path>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// Inverted Index
		runJob(args);

		return 0;
	}

	private void runJob(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job1 = Job.getInstance(new Configuration(getConf()),
				"Inverted Index");
		job1.setJarByClass(InvertedIndexDriver.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(InvertedIndexReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(NullWritable.class);
		job1.setOutputValueClass(PageCountWritable.class);

		job1.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new InvertedIndexDriver(), args);
		System.exit(exitCode);
	}

}
