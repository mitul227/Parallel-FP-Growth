package parallelCounting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParallelCountingReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
	private final static LongWritable result = new LongWritable();
	
	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
		
		Configuration conf = context.getConfiguration();
		int minSupport = Integer.parseInt(conf.get("minSupport"));
		
		long sum = 0;
		for(LongWritable val : values) {
			sum += val.get();
		}
		
		if(sum >= minSupport) {
			result.set(sum);
			context.write(key, result);
		}
	}
}
