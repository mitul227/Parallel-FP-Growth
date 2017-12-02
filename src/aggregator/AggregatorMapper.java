package aggregator;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AggregatorMapper extends Mapper<Object,Text,Text,Text>{
	
	private final static Text newKey = new Text();
	private final static Text newValue = new Text();
	
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		String fields[] = value.toString().split(":");
		fields[0] = fields[0].trim();
		String pattern = fields[0].substring(1,fields[0].length()-1);
		long count = Long.parseLong(fields[1].trim());
		StringTokenizer items = new StringTokenizer(pattern,",");
		while(items.hasMoreTokens()) {
			String item = items.nextToken().trim();
			newKey.set(item);
			newValue.set("["+pattern.trim()+"]"+":"+count);
			context.write(newKey,newValue);
		}
	}
}
