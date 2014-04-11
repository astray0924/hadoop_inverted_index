import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, IntWritable, Text, CustomArrayWritable> {

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		List<IntWritable> list = new ArrayList<IntWritable>();
		for (IntWritable val : values) {
			if (!list.contains(val)) {
				list.add(new IntWritable(val.get()));
			}
		}

		context.write(
				key,
				new CustomArrayWritable(IntWritable.class, list
						.toArray(new IntWritable[list.size()])));

	}

}