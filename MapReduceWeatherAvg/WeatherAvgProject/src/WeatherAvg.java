import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAvg{

	
 public static void main(String[] args) throws Exception {
   if(args.length!=2) {
	   System.err.println("Usage: Weather Average <input path> <output path>");
	   System.exit(-1);
   }
   
   Configuration conf = new Configuration();
   Job job = new Job();
   job.setJarByClass(WeatherAvg.class);
   job.setJobName("Weather Average");
   job.setMapperClass(WeatherAvgMapper.class);
   //job.setCombinerClass(IntSumReducer.class);
   job.setReducerClass(WeatherAvgReducer.class);
   job.setNumReduceTasks(2);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }

}
