package utils;

import java.util.ArrayList;
import java.util.List;


public class FpTreeNode {
	private String itemName;
	private long count;
	private List<FpTreeNode> children;
	private FpTreeNode parent;
	private FpTreeNode nextLink;
	
	public FpTreeNode(String itemName,long count) {
		this.itemName = itemName;
		this.count = count;
		this.children = new ArrayList<FpTreeNode>();
		this.parent = null;
		this.nextLink = null;
	}
	
	public void setCount(long count) {
		this.count = count;
	}
	
	public void setParent(FpTreeNode parent) {
		this.parent = parent;
	}
	
	public void setNextLink(FpTreeNode nextLink) {
		this.nextLink = nextLink;
	}
	
	public String getItemName() {
		return this.itemName;
	}
	
	public long getCount() {
		return this.count;
	}
	
	public FpTreeNode getParent() {
		return this.parent;
	}
	
	public FpTreeNode getNextLink() {
		return this.nextLink;
	}
	
	public List<FpTreeNode> getChildren() {
		return this.children;
	}
	
	public int getChildrenCount() {
		return children.size();
	}
	
	public void addChild(FpTreeNode n) {
		children.add(n);
	}
	
	/* check if node contains a child having item as itemName and returns that child */
	public FpTreeNode isChildContains(String itemName) {
		for(int i=0;i<children.size();i++){
			if(children.get(i).getItemName().equals(itemName)) {
				return children.get(i);
			}
		}
		return null;
	}
	
	/* check if node has only one child */
	public boolean hasOnlyOneChild() {
		if(children.size() == 1)
			return true;
		else
			return false;
	}
}
