package utils;

import java.util.List;

public class FpTree {
	private FpTreeNode root;
	private List<HeaderEntry> HeaderTable;
	
	public FpTree(FpTreeNode root,List<HeaderEntry> HeaderTable) {
		this.root = root;
		this.HeaderTable = HeaderTable;
	}

	public FpTreeNode getRoot() {
		return this.root;
	}

	public List<HeaderEntry> getHeaderTable() {
		return this.HeaderTable;
	}
}
