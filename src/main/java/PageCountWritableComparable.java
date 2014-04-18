import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PageCountWritableComparable implements WritableComparable<Object> {
	private Text keyword;
	private LongWritable count;

	public PageCountWritableComparable() {
		this.keyword = new Text("");
		this.count = new LongWritable(0);
	}

	public PageCountWritableComparable(Text keyword, LongWritable count) {
		this.keyword = keyword;
		this.count = count;
	}

	@Override
	public String toString() {
		return String.format("%s %s", keyword, count);
	}

	public void write(DataOutput out) throws IOException {
		keyword.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		keyword.readFields(in);
		count.readFields(in);

	}

	public Text getKeyword() {
		return keyword;
	}

	public void setKeyword(Text keyword) {
		this.keyword = keyword;
	}

	public LongWritable getCount() {
		return count;
	}

	public void setCount(LongWritable count) {
		this.count = count;
	}

	public int compareTo(Object o) {
		PageCountWritableComparable compareTo = (PageCountWritableComparable) o;

		int result = count.compareTo(compareTo.getCount());

		if (result == 0) {
			result = keyword.compareTo(compareTo.getKeyword());
		}

		return -result;
	}

}
