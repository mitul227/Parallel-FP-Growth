package utils;

import java.util.Comparator;

/* Sort by count in descending order */

public class SortByCount implements Comparator<Pair>{
	
	public int compare(Pair a, Pair b) {
		if(a.getCount() == b.getCount()) {
			if(a.getItem().compareTo(b.getItem()) > 0)
				return 1;
			else
				return -1;
		}
		else if(a.getCount() > b.getCount())
			return -1;
		else
			return 1;
	}
}
