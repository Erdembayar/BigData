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

public class WeatherAvgReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
		
		int sum = 0;
		int count = 0;
		for(IntWritable value: values){
			sum+= value.get();
			count++;
		}
		double avg =  count ==0 ? 0: sum/count;
		context.write(key, new DoubleWritable(avg/10)); //We're dividing by 10 to compensating multification of orig data
	}

}
