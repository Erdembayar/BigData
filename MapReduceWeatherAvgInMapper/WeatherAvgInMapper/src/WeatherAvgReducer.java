import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAvgReducer extends Reducer<Text, MyResult, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<MyResult> values, Context context)
		throws IOException, InterruptedException{
		System.out.println("Reducing year:"+ key);
		int sum = 0;
		int count = 0;
		System.out.println(values);
		if(values==null)
			return;
		for(MyResult value: values){
			sum = value.getTemp().get();
			System.out.println("Sum:"+sum);
			count =value.getCount().get();
			System.out.println("Count:"+count);
		}
		double avg =  count ==0 ? 0: sum/count;
		context.write(key, new DoubleWritable(avg/10)); //We're dividing by 10 to compensating multification of orig data
	}

}
