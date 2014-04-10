package invertedindex;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexer {
	final static Charset ENCODING = StandardCharsets.UTF_8;

	private static List<Text> stopWords;

	public static class Map extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String page = value.toString();

			System.out.println(page);
		}
		// Input - key: 오프셋, value: 줄의 내용
		// Output - key: page ID, value: 키워드
	}

	public static class Reduce extends
			Reducer<LongWritable, Text, Text, List<IntWritable>> {

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
		public IntWritable[] get() {
			return (IntWritable[]) super.get();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			for (IntWritable i : get()) {
				i.write(arg0);
			}
		}
	}

	public static void main(String[] args) throws IOException {
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
		// Job job = new Job();
		// job.setJarByClass(InvertedIndexer.class);
		// job.setJobName("Build Inverted Index");
		//
		// FileInputFormat.addInputPath(job, new Path("args[0]"));
		// FileOutputFormat.setOutputPath(job, new Path("args[1]"));
		//
		// job.setMapperClass(Mapper.class);
		// job.setReducerClass(Reducer.class);
		//
		// job.setMapOutputKeyClass(LongWritable.class);
		// job.setMapOutputValueClass(Text.class);
		//
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(PageIDArrayWritable.class);

	}

}
