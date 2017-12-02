package parallelFpGrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import main.FpGrowth;
import utils.FpTree;
import utils.Helper;
import utils.ListPair;
import utils.Pair;

public class ParallelFpReducer extends Reducer <IntWritable,Text,Text,LongWritable>{
	private final static LongWritable value = new LongWritable();
	private static List<Pair> fList = new ArrayList<Pair>();
	private final static Text newKey = new Text();
	
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		Configuration conf = context.getConfiguration();
		int minSupport = Integer.parseInt(conf.get("minSupport"));
		int noOfGroups = Integer.parseInt(conf.get("noOfGroups"));
		int k = Integer.parseInt(conf.get("maxPatterns"));
		List<Pair> localFlist = new ArrayList<Pair>();
		ArrayList<String> transactions = new ArrayList<String>();
		List<String> localGList = new ArrayList<String>();
		Map<String,Integer> gList = new HashMap<String,Integer>();
		//Map<String,Boolean> localGList = new HashMap<String,Boolean>();
		
		for(Text val : values) {
			transactions.add(val.toString());
		}
		
		int totalElements = fList.size();
		if(noOfGroups >= totalElements) {
			for(int i=0;i<totalElements;i++) {
				gList.put(fList.get(i).getItem(), i+1);
				if(i+1 == key.get())
					localGList.add(fList.get(i).getItem());
					//localGList.put(fList.get(i).getItem(),true);
			}
		}
		else {
			int elePerGroup = totalElements/noOfGroups;
			for(int i=0;i<totalElements;i++) {
				int groupId = (i+1)/elePerGroup;
				if((i+1)%elePerGroup != 0) {
					groupId++;
				}
				if(groupId > noOfGroups)
					groupId = noOfGroups;
				gList.put(fList.get(i).getItem(), groupId);
				if(groupId == key.get())
					localGList.add(fList.get(i).getItem());
			}
		}
		
		localFlist = Helper.makeFList(transactions, minSupport,gList);
		
		/* construct fp tree */
		FpTree tree = Helper.constructFpTree(transactions,localFlist);
		
		//List<ListPair> patterns = new ArrayList<ListPair>();7
		
		for(int i=0;i<localGList.size();i++) {
			List<ListPair> patterns = new ArrayList<ListPair>();
			//System.out.println(key.get() + " - " + localGList.get(i) + "--");
			patterns = Helper.mineFrequentPatterns(tree, minSupport, localGList.get(i), null,true);
			
			System.out.println(localGList.get(i) + " -- ");
			
			for(int j=0;j<patterns.size();j++) {
				Collections.sort(patterns.get(j).getItems());
				System.out.println(j + " - " + patterns.get(j).getItems().toString() + " : " + patterns.get(j).getCount());
			
			}
			
			/* add all patterns and their counts to a max heap */
			PriorityQueue<ListPair> maxHeap = new PriorityQueue<ListPair>(new Comparator<ListPair>() {
				public int compare(ListPair p1,ListPair p2) {
					if(p1.getCount() < p2.getCount())
						return 1;
					else
						return -1;
					}
			});
			for(int j=0;j<patterns.size();j++) {
				maxHeap.add(patterns.get(j));
			}
			int curr = 0;
			while(!maxHeap.isEmpty() && curr < k) {
				ListPair p = maxHeap.poll();
				newKey.set(p.getItems().toString() + " : ");
				value.set(p.getCount());
				context.write(newKey, value);
				curr++;
			}
		}	
	}
	
	/* read F-list */
	public void setup(Context context) throws IOException,InterruptedException{
		fList = FpGrowth.readFList(context.getConfiguration());
	}

}
