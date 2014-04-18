import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {
	public static final String OUTPUT_INVERTED_INDEX = "/Users/kyoungrok/Desktop/"
			+ "output_inverted_index";

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
//		runInvertedIndexJob(args);

		// Index count
		runCountIndexJob(args);

		return 0;
	}

	private void runCountIndexJob(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Configuration conf2 = new Configuration(getConf());
		Job job2 = Job.getInstance(conf2, "index count");
		job2.setJarByClass(InvertedIndexDriver.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT_INVERTED_INDEX));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		job2.setMapperClass(CountIndexMapper.class);
		job2.setReducerClass(CountIndexReducer.class);

		job2.setMapOutputKeyClass(PageCountWritableComparable.class);
		job2.setMapOutputValueClass(LongWritable.class);

		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(PageCountWritableComparable.class);

		job2.setInputFormatClass(SequenceFileInputFormat.class);

		// wait for completion
		job2.waitForCompletion(true);
	}

	private void runInvertedIndexJob(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		Job job1 = Job.getInstance(new Configuration(getConf()),
				"Inverted Index");
		job1.setJarByClass(InvertedIndexDriver.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_INVERTED_INDEX));

		job1.setMapperClass(InvertedIndexMapper.class);
		job1.setReducerClass(InvertedIndexReducer.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(LongWritable.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongArrayWritable.class);

		job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		job1.waitForCompletion(true);
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int exitCode = ToolRunner.run(conf, new InvertedIndexDriver(), args);
		System.exit(exitCode);
	}

}
