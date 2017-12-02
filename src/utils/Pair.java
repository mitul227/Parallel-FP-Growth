package utils;

public class Pair {
	private String item;
	private long count;
	
	public Pair(String item,long count){
		this.item = item;
		this.count = count;
	}
	
	public String getItem() {
		return item;
	}
	
	public long getCount() {
		return count;
	}
	
	public void setCount(long newCount) {
		this.count = newCount;
	}
}
