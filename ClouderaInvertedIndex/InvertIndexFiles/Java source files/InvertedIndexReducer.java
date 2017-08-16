import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends
		Reducer<Text, IntWritable, Text, Text> {
	private TreeMap<String, String> map;
	//private Text wordWritable = new Text(""); 
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		System.out.println("Reducing key: pairs= " + key);
		TreeMap<String, String> map = getMap();
		System.out.println(values);
		if (values == null)
			return;

		for (IntWritable value : values) {
			String word= key.toString().substring(0, key.toString().indexOf(" "));
			word = word.trim();
			//wordWritable.set(word);
			System.out.println("Word:"+word+ " length:"+word.length());
			String docId= key.toString().substring(key.toString().indexOf(" "));
			docId= docId.trim();
			
			System.out.println("docId:"+docId+ " length:"+docId.length());
			if(map.containsKey(word)){
				//String val = map.get(key.getWord()).toString();
				String val = map.get(word);
				val += "; (";
				val += docId;
				val += ",";
				val += value;
				val += ") ";
				System.out.println(val);
				map.put(word, val);
			}
			else{
				String val = " -> (";
				val += docId;
				val += ",";
				val += value;
				val += ") ";
				System.out.println(val);
				map.put(word,val);
			}

		}

	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		TreeMap<String, String> map = getMap();
		for(Entry<String, String> val: map.entrySet()){
			String sKey = val.getKey();
			context.write(new Text(sKey), new Text(val.getValue()));
			
		}

	}

	public TreeMap<String, String> getMap() {
		if (null == map)
			map = new TreeMap<String, String>();
		return map;
	}
}
