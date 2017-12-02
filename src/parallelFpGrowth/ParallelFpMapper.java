package parallelFpGrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import main.FpGrowth;
import utils.Helper;
import utils.Pair;

public class ParallelFpMapper extends Mapper<Object,Text,IntWritable,Text>{
	private static List<Pair> fList = new ArrayList<Pair>();
	private final static IntWritable newKey = new IntWritable();
	private final static Text newValue = new Text();
	
	public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
		/* Arrange transaction according to F-List */
		String transaction = new String(value.toString());
		
		Map<String,Integer> gList = new HashMap<String,Integer>();
		Map<Integer,Boolean> visited = new HashMap<Integer,Boolean>();
		
		Configuration conf = context.getConfiguration();
		int noOfGroups = Integer.parseInt(conf.get("noOfGroups"));
		
		int totalElements = fList.size();
		if(noOfGroups >= totalElements) {
			for(int i=0;i<totalElements;i++) {
				gList.put(fList.get(i).getItem(), i+1);
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
				//System.out.println(fList.get(i).getItem() + " -m- " + groupId);
				//System.out.println(l.get(i) + " : " + g);
			}
		}
		
		
		/* reorder transaction according to F-list */
		List<String> newTransactionList = Helper.reorderTransaction(transaction, fList);
		
		int size = newTransactionList.size();
		for(int j = size-1;j >= 0;j--) {
			int gid = gList.get(newTransactionList.get(j));
			if(!visited.containsKey(gid)) {
				visited.put(gid, true);
				newKey.set(gid);
				StringBuilder dependentTransactions = new StringBuilder();
				for(int i = 0;i <= j;i++) {
					dependentTransactions.append(newTransactionList.get(i));
					//dependentTransactions += newTransactionList.get(i);
					if(i != j)
						dependentTransactions.append(" ");
						//dependentTransactions += " ";
				}
				newValue.set(dependentTransactions.toString());
				context.write(newKey, newValue);
			}
			//newKey.set(newTransactionList.get(j));
		}
	}
	
	/* read F-list */
	public void setup(Context context) throws IOException,InterruptedException{
		fList = FpGrowth.readFList(context.getConfiguration());
	}

}
