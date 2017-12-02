package utils;

import java.util.List;

public class ListPair {
	private List<String> items;
	private long count;
	
	public ListPair(List<String> items,long count){
		this.items = items;
		this.count = count;
	}
	
	public List<String> getItems() {
		return items;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long newCount) {
		this.count = newCount;
	}
	
	public void addItem(String item) {
		items.add(item);
	}
}
