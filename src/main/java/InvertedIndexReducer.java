import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, LongWritable, Text, CustomArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		List<LongWritable> list = new ArrayList<LongWritable>();
		for (LongWritable val : values) {
			if (!list.contains(val)) {
				list.add(new LongWritable(val.get()));
			}
		}

		context.write(
				key,
				new CustomArrayWritable(LongWritable.class, list
						.toArray(new LongWritable[list.size()])));

	}

}