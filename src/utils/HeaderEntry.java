package utils;


public class HeaderEntry {
	private String itemName;
	private long totalCount;
	private FpTreeNode headLink;
	
	public HeaderEntry(String itemName,FpTreeNode headLink) {
		this.itemName = itemName;
		this.headLink = headLink;
	}
	
	public FpTreeNode getHeadLink() {
		return headLink;
	}
	
	public String getItemName() {
		return itemName;
	}
	
	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}
	
	public long getTotalCount() {
		return totalCount;
	}
}
