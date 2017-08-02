import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Writable;


public final class MyResult implements Writable{
	private  IntWritable count;
	private  IntWritable temp;

	
	public MyResult() {
		this.count = new IntWritable();
		this.temp = new IntWritable();
	}


	public MyResult(IntWritable count, IntWritable temp) {
		super();
		this.count = count;
		this.temp = temp;
	}

	public IntWritable getCount() {
		return count;
	}


	public void setCount(IntWritable count) {
		this.count = count;
	}


	public IntWritable getTemp() {
		return temp;
	}


	public void setTemp(IntWritable temp) {
		this.temp = temp;
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		count.readFields(in);
		temp.readFields(in);
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		count.write(out);
		temp.write(out);
	}

}
