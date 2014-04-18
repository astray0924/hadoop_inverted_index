import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PageCountWritable implements Writable {
	private Text keyword;
	private IntWritable count;

	public PageCountWritable() {
		this.keyword = new Text("");
		this.count = new IntWritable(0);
	}

	public PageCountWritable(String keyword, int count) {
		this.keyword = new Text(keyword);
		this.count = new IntWritable(count);
	}
	
	public PageCountWritable(Text keyword, IntWritable count) {
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

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

//	public int compareTo(Object o) {
//		PageCountWritableComparable compareTo = (PageCountWritableComparable) o;
//
//		int result = count.compareTo(compareTo.getCount());
//
//		if (result == 0) {
//			result = keyword.compareTo(compareTo.getKeyword());
//		}
//
//		return -result;
//	}

}
