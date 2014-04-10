package invertedindex;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexer {
	private static List<Text> stopWords;

	public static class InvertedIndexerMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		private static final Pattern sanitizePattern = Pattern.compile("[^A-Za-z0-9 ]");
		private static final Pattern pageIdPattern = Pattern.compile("<id>(.+?)</id>");

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String page = value.toString();
			
			String pageIdString = getPageID(page);
			
			System.out.println(pageIdString);
			
			String sanitizedPage = getSanitizedPage(page);
			
			StringTokenizer iter = new StringTokenizer(sanitizedPage.toLowerCase());

			while (iter.hasMoreTokens()) {
				Text word = new Text();
				word.set(iter.nextToken());

				LongWritable pageId = new LongWritable(new Long(pageIdString));

				if (stopWords.contains(word)) {
					context.write(pageId, word);					
				}
			}
		}

		public String getSanitizedPage(String page) {
			return sanitizePattern.matcher(page).replaceAll("");
		}
		
		public String getPageID(String page) {
			return pageIdPattern.matcher(page).group(1);
		}
	}

	public static class InvertedIndexerReducer extends
			Reducer<LongWritable, Text, Text, List<IntWritable>> {

		@Override
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) {

		}

	}

	class PageIDArrayWritable extends ArrayWritable {

		public PageIDArrayWritable(Class<? extends Writable> valueClass,
				Writable[] values) {
			super(valueClass, values);
		}

		public PageIDArrayWritable(Class<? extends Writable> valueClass) {
			super(valueClass);
		}

		@Override
		public LongWritable[] get() {
			return (LongWritable[]) super.get();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			for (LongWritable i : get()) {
				i.write(arg0);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// arguments
		if (args.length != 2) {
			System.err
					.println("Usage: InvertedIndexer <input path> <output path>");
			System.exit(-1);
		}

		// stopword 리스트 생성
		stopWords = new ArrayList<Text>();
		BufferedReader br = new BufferedReader(new FileReader(new File(
				"stopwords_v3.txt")));
		String line;
		while ((line = br.readLine()) != null) {
			stopWords.add(new Text(line));
		}

		// 하둡 Job 생성 및 실행
		Job job = new Job();
		job.setJarByClass(InvertedIndexer.class);
		job.setJobName("Build Inverted Index");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InvertedIndexerMapper.class);
		job.setReducerClass(InvertedIndexerReducer.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(PageIDArrayWritable.class);

		job.waitForCompletion(true);

	}

}
