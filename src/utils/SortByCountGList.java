package utils;

import java.util.Comparator;
import java.util.Map;

public class SortByCountGList implements Comparator<Pair>{
	private Map<String,Integer> gList;
	
	public SortByCountGList(Map<String,Integer> gList) {
		this.gList = gList;
	}
	
	public int compare(Pair a, Pair b) {
		if(a.getCount() == b.getCount()) {
			int groupIdA = gList.get(a.getItem());
			int groupIdB = gList.get(b.getItem());
			if(groupIdA < groupIdB)
				return -1;
			else if(groupIdA > groupIdB)
				return 1;
			else {
				if(a.getItem().compareTo(b.getItem()) > 0)
					return 1;
				else
					return -1;
			}
		}
		else if(a.getCount() > b.getCount())
			return -1;
		else
			return 1;
	}
}