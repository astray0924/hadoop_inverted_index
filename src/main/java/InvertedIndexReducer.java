import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, LongWritable, NullWritable, PageCountWritable> {

	@Override
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		int c = 0;
		Iterator<LongWritable> iter = values.iterator();
		while (iter.hasNext()) {
			iter.next();
			c += 1;
		}

		PageCountWritable pageCount = new PageCountWritable(
				key.toString(), c);
		context.write(NullWritable.get(), pageCount);

	}
}