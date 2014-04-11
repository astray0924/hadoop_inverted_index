package invertedindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndexDriver extends Configured implements Tool {

	// <word>: <page IDs>
	public static class Reduce extends
			Reducer<Text, IntWritable, Text, PageIdArrayWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			List<IntWritable> list = new ArrayList<IntWritable>();
			for (IntWritable val : values) {
				if (!list.contains(val)) {
					list.add(new IntWritable(val.get()));
				}
			}

			context.write(
					key,
					new PageIdArrayWritable(IntWritable.class, list
							.toArray(new IntWritable[list.size()])));

		}

	}

	public static class PageIdArrayWritable extends ArrayWritable {

		public PageIdArrayWritable(Class<? extends Writable> valueClass,
				Writable[] values) {
			super(valueClass, values);
		}

		public PageIdArrayWritable(Class<? extends Writable> valueClass) {
			super(valueClass);
		}

		@Override
		public Writable[] get() {
			return (Writable[]) super.get();
		}

		@Override
		public String toString() {
			return Arrays.toString(get());
		}
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new InvertedIndexDriver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		// arguments
		if (args.length < 2) {
			System.err
					.printf("Usage: InvertedIndexBuilder [generic options] <input path> <output path>\n",
							getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}

		// 하둡 Job 생성 및 실행
		Job job = new Job();
		job.setJarByClass(InvertedIndexDriver.class);
		job.setJobName("Build Inverted Index");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageIdArrayWritable.class);

		job.setNumReduceTasks(3);
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
