import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherAvgMapper extends
		Mapper<LongWritable, Text, Text, MyResult> {

	private Map<String,MyResult> map;
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Mapping start");
		Map<String,MyResult> map= getMap();
		StringTokenizer itr = new StringTokenizer(value.toString());
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		int count =0;
		while(itr.hasMoreTokens()){
			System.out.println("line"+line);
			line = itr.nextToken();
			year = line.substring(15, 19);
			
			System.out.println("Increased counter:"+count);
			if (line.charAt(87) == '+') {
				airTemperature = Integer.parseInt(line.substring(88, 92));
			} else {
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}

			System.out.println("Year:"+year+ " Temp:"+airTemperature);
			if(map.containsKey(year)) {
				int total = map.get(year).getTemp().get()+airTemperature;
				count = map.get(year).getCount().get()+1;
				map.put(year, new MyResult(new IntWritable(count), new IntWritable(total)));
			} else {
				
				map.put(year, new MyResult(new IntWritable(1), new IntWritable(airTemperature)));
			}
		}
		
		// System.out.println("Temperature :"+ year+"  "+ airTemperature);
		// context.write(new Text(year), new IntWritable(airTemperature));

	}
	
	protected void cleanup(Context context)
		throws IOException, InterruptedException{
		Map<String,MyResult> map= getMap();
		Iterator<Map.Entry<String,MyResult>> it =map.entrySet().iterator();
		while(it.hasNext()) {
			Map.Entry<String, MyResult> entry = it.next();
			String sKey = entry.getKey();
			int total = entry.getValue().getTemp().get();
			System.out.println("Cleanup:"+ sKey+ " "+ entry.getValue().getCount()+" "+entry.getValue().getTemp());
			context.write(new Text(sKey), entry.getValue());
		}
	}
	public Map<String, MyResult> getMap() {
		if(null== map)
			map= new HashMap<String,MyResult>();
		return map;
	}
}
