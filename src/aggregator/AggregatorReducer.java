package aggregator;

import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.Pair;


public class AggregatorReducer extends Reducer<Text,Text,Text,Text>{
	
	private final static Text value = new Text();
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		StringBuilder itemsets = new StringBuilder();
		
		Configuration conf = context.getConfiguration();
		int k = Integer.parseInt(conf.get("maxPatterns"));
		
		PriorityQueue<Pair> maxHeap = new PriorityQueue<Pair>(new Comparator<Pair>() {
			public int compare(Pair p1,Pair p2) {
				if(p1.getCount() < p2.getCount())
					return 1;
				else
					return -1;
			}
		});
		
		for(Text val : values) {
			String fields[] = val.toString().split(":");
			String pattern = fields[0].trim();
			long count = Long.parseLong(fields[1].trim());
			maxHeap.add(new Pair(pattern,count));
		}
		
		int curr = 0;
		while(!maxHeap.isEmpty() && curr < k) {
			Pair p = maxHeap.poll();
			itemsets.append("(" + p.getItem() + " : " + p.getCount() + ")");
			if(curr != k-1 && maxHeap.size() != 0)
				itemsets.append(" , ");
			curr++;
		}
		value.set(itemsets.toString());
		context.write(key, value);
	}
}
