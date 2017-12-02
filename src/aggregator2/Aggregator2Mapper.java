package aggregator2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Aggregator2Mapper extends Mapper<Object,Text,Text,LongWritable>{
	
	private final static Text newKey = new Text();
	private final static LongWritable newValue = new LongWritable();
	
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		String fields[] = value.toString().split(":");
		fields[0] = fields[0].trim();
		fields[1] = fields[1].trim();
		newKey.set(fields[0]);
		newValue.set(Long.parseLong(fields[1]));
		context.write(newKey, newValue);
	}
}