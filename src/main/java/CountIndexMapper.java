import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountIndexMapper
		extends
		Mapper<Text, LongArrayWritable, PageCountWritableComparable, LongWritable> {
	
	public void setup(Context context) throws IOException {
		
	}

	@Override
	public void map(Text keyword, LongArrayWritable indices, Context context) throws IOException, InterruptedException {
		LongWritable count = new LongWritable(indices.get().length);
		PageCountWritableComparable pcKey = new PageCountWritableComparable(
				keyword, count);
		
		context.write(pcKey, count);
	}
}
