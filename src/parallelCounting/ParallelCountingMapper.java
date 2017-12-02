package parallelCounting;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParallelCountingMapper extends Mapper<Object,Text,Text,LongWritable>{
	
	private final static LongWritable one = new LongWritable(1);
	private Text word = new Text();
	
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		StringTokenizer str = new StringTokenizer(value.toString());
		while(str.hasMoreTokens()) {
			word.set(str.nextToken());
			context.write(word, one);
		}
	}
}
