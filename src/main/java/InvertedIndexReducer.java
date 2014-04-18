import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, LongWritable, Text, LongArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		List<LongWritable> list = new ArrayList<LongWritable>();
		for (LongWritable val : values) {
			if (!list.contains(val)) {
				list.add(new LongWritable(val.get()));
			}
		}

		LongArrayWritable indices = new LongArrayWritable();
		indices.set(list.toArray(new LongWritable[list.size()]));
		context.write(key, indices);

	}
}