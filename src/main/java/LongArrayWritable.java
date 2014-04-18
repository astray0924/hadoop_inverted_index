import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	@Override
	public void set(Writable[] values) {
		super.set(values);
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