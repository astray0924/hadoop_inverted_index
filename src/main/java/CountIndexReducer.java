import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountIndexReducer
		extends
		Reducer<PageCountWritableComparable, LongWritable, NullWritable, PageCountWritableComparable> {

	@Override
	public void reduce(PageCountWritableComparable key,
			Iterable<LongWritable> indices, Context context)
			throws IOException, InterruptedException {
//		Text keyword = key.getKeyword();
//		LongWritable count = indices.iterator().next();

		context.write(NullWritable.get(), key);
	}
}
