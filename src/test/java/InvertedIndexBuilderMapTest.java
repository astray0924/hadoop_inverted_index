
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

public class InvertedIndexBuilderMapTest {
	@Test
	public void testValidRecordParsing() throws IOException,
			InterruptedException {
		Text value = new Text("<id>10</id> test test2 test3-");

		MapDriver<LongWritable, Text, Text, IntWritable> driver = new MapDriver<LongWritable, Text, Text, IntWritable>();

		driver.withMapper(new InvertedIndexMapper())
				.withInput(new LongWritable(1), value)
				.withOutput(
						new Pair<Text, IntWritable>(new Text("10"),
								new IntWritable(10)))
				.withOutput(new Text("test"), new IntWritable(10))
				.withOutput(new Text("test2"), new IntWritable(10))
				.withOutput(new Text("test3"), new IntWritable(10));

		driver.runTest();
	}

	@Test
	public void testMultiPageIdCase() throws IOException, InterruptedException {
		Text value = new Text("<id>10</id> <id>99</id> test test2 test3-");

		MapDriver<LongWritable, Text, Text, IntWritable> driver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		
		driver.withMapper(new InvertedIndexMapper())
				.withInput(new LongWritable(1), value)
				.withOutput(
						new Pair<Text, IntWritable>(new Text("10"),
								new IntWritable(10)))
				.withOutput(new Text("99"), new IntWritable(10))
				.withOutput(new Text("test"), new IntWritable(10))
				.withOutput(new Text("test2"), new IntWritable(10))
				.withOutput(new Text("test3"), new IntWritable(10));

		driver.runTest();
	}
}
