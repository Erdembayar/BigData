import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	private Map<Text, IntWritable> map;
	private Text word = new Text();
	private Text docId = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		System.out.println("Mapping start");
		Map<Text, IntWritable> map = getMap();

		String line = value.toString();
		System.out.println("Whole line: " + line);
		docId.set(line.substring(8, 11));
		System.out.println("docid: " + docId);
		String docContent = line.substring(13);
		System.out.println("docContent: " + docContent);
		StringTokenizer itrByTerms = new StringTokenizer(docContent);
		String wordBuffer;
		int count = 0;
		while (itrByTerms.hasMoreTokens()) {
			line = itrByTerms.nextToken().trim();
			
			if (line == "[" || line == "]" || line=="" || line.length()<=2)
			{
				System.out.println("Skipping:"+line);
				continue;
			}
			System.out.println("Processing line: " + line);
			
			word.set(line);
			Text currentPair = new Text(word+" "+docId);
			
			if (map.containsKey(currentPair)) {
				//System.out.println("Contains Current pair: " + currentPair.getWord()+" " +currentPair.getDocId());
				count = map.get(currentPair).get() + 1;
				System.out.println("Putting into Hash: word="+ currentPair+" count="+count);
				map.put(currentPair, new IntWritable(count));
			} else {
				//System.out.println("New Current pair: " + currentPair.getWord()+" " +currentPair.getDocId());
				count =1;
				System.out.println("Putting into Hash: word="+ currentPair+ " count="+count);
				map.put(currentPair, new IntWritable(count));
			}

		}
		

	}

	 protected void cleanup(Context context)
	 throws IOException, InterruptedException{
		 	System.out.println("Clean up .........");
		    Map<Text, IntWritable> map= getMap();
			System.out.println("Emit length = "+ map.size());
			
			Iterator<Map.Entry<Text,IntWritable>> it =map.entrySet().iterator();
			while(it.hasNext()) {
				Map.Entry<Text,IntWritable> entry = it.next();
				Text sKey = entry.getKey();
				IntWritable val = entry.getValue();
	            System.out.println("Emitting key: "+ sKey+ " value: "+val);

				context.write(sKey, entry.getValue());

			}

	 }
	public Map<Text, IntWritable> getMap() {
		if (null == map)
			map = new HashMap<Text, IntWritable>();
		return map;
	}
}
