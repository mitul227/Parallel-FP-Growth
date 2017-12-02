package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;


public class Helper {
	
	/* make f-list according to transactions and min Support */
	public static List<Pair> makeFList(ArrayList<String> transactions,int minSupport,Map<String,Integer> gList) {
		Map<String,Integer> itemsAndIndex = new HashMap<String,Integer>();
		List<Pair> fList = new ArrayList<Pair>();
		int currIndex = 0;
		for(String t : transactions) {
			StringTokenizer str = new StringTokenizer(t);
			while(str.hasMoreTokens()) {
				String itemName = str.nextToken();
				if(itemsAndIndex.containsKey(itemName)) {
					int index = itemsAndIndex.get(itemName);
					long oldCount = fList.get(index).getCount();
					fList.get(index).setCount(oldCount+1);
				}
				else {
					fList.add(new Pair(itemName,1));
					itemsAndIndex.put(itemName, currIndex);
					currIndex++;
				}
			}
		}
		Collections.sort(fList,new SortByCountGList(gList));
		List<Pair> finalFList = new ArrayList<Pair>();
		for(int i=0;i<fList.size();i++) {
			if(fList.get(i).getCount() >= minSupport)
				finalFList.add(fList.get(i));
			else
				break;
		}
		return finalFList;
	}
	
	/* make f-list according to conditional pattern base */
	public static List<Pair> makeFList(List<Pair> conditionalPatternBase,int minSupport) {
		Map<String,Integer> itemsAndIndex = new HashMap<String,Integer>();
		List<Pair> fList = new ArrayList<Pair>();
		int currIndex = 0;
		for(int i=0;i<conditionalPatternBase.size();i++) {
			String path = conditionalPatternBase.get(i).getItem();
			long count = conditionalPatternBase.get(i).getCount();
			StringTokenizer str = new StringTokenizer(path);
			while(str.hasMoreTokens()) {
				String itemName = str.nextToken();
				if(itemsAndIndex.containsKey(itemName)) {
					int index = itemsAndIndex.get(itemName);
					long oldCount = fList.get(index).getCount();
					fList.get(index).setCount(count+oldCount);
				}
				else {
					fList.add(new Pair(itemName,count));
					itemsAndIndex.put(itemName, currIndex);
					currIndex++;
				}
			}
		}
		Collections.sort(fList,new SortByCount());
		List<Pair> finalFList = new ArrayList<Pair>();
		for(int i=0;i<fList.size();i++) {
			if(fList.get(i).getCount() >= minSupport)
				finalFList.add(fList.get(i));
			else
				break;
		}
		return finalFList;
	}
	
	/* reorder a given transaction according to f-list */
	public static List<String> reorderTransaction(String transaction,List<Pair> fList) {
		StringTokenizer str = new StringTokenizer(transaction);
		Map<String,Boolean> items = new HashMap<String,Boolean>();
		while(str.hasMoreTokens()) {
			items.put(str.nextToken(),true);
		}
		List<String> newTransactionList = new ArrayList<String>();
		for(Pair itemAndCount : fList) {
			if(items.containsKey(itemAndCount.getItem())) {
				newTransactionList.add(itemAndCount.getItem());
			}
		}
		return newTransactionList;
	}
	
	/* construct fp tree using transactions and f-list */
	public static FpTree constructFpTree(List<String> transactions,List<Pair> fList) {
		FpTreeNode root = new FpTreeNode(null,-1);
		List<HeaderEntry> headerTable = new ArrayList<HeaderEntry>();
		Map<String,FpTreeNode> headerMap = new HashMap<String,FpTreeNode>();
		for(String t : transactions) {
			List<String> ordered = reorderTransaction(t,fList);
			//System.out.println(ordered);
			FpTreeNode currNode = root;
			for(String item : ordered) {
				FpTreeNode child = currNode.isChildContains(item);
				if(child != null) {
					long count = child.getCount();
					child.setCount(count+1);
					currNode = child;
				}
				else {
					FpTreeNode n = new FpTreeNode(item,1);
					if(headerMap.containsKey(item)) {
						headerMap.get(item).setNextLink(n);
						headerMap.put(item,n);
					}
					else {
						HeaderEntry h = new HeaderEntry(item,n);
						headerMap.put(item, n);
						headerTable.add(h);
					}
					n.setParent(currNode);
					currNode.addChild(n);
					currNode = n;
				}
			}
		}
		for(int i=0;i<headerTable.size();i++) {
			FpTreeNode n = headerTable.get(i).getHeadLink();
			long count = 0;
			while(n != null) {
				count += n.getCount();
				n = n.getNextLink();
			}
			headerTable.get(i).setTotalCount(count);
		}
		FpTree tree = new FpTree(root,headerTable); 
		return tree;
	}
	
	/* construct fp tree using transactions and f-list and a count-list (using conditional pattern base) */
	public static FpTree constructFpTree(List<String> transactions,List<Pair> fList,List<Long> countList) {
		FpTreeNode root = new FpTreeNode(null,-1);
		List<HeaderEntry> headerTable = new ArrayList<HeaderEntry>();
		Map<String,FpTreeNode> headerMap = new HashMap<String,FpTreeNode>();
		for(int i=0;i<transactions.size();i++) {
			String t = transactions.get(i);
			List<String> ordered = reorderTransaction(t,fList);
			//System.out.println(ordered);
			FpTreeNode currNode = root;
			for(String item : ordered) {
				FpTreeNode child = currNode.isChildContains(item);
				if(child != null) {
					long oldCount = child.getCount();
					long count = countList.get(i);
					child.setCount(oldCount + count);
					currNode = child;
				}
				else {
					FpTreeNode n = new FpTreeNode(item,countList.get(i));
					if(headerMap.containsKey(item)) {
						headerMap.get(item).setNextLink(n);
						headerMap.put(item,n);
					}
					else {
						HeaderEntry h = new HeaderEntry(item,n);
						headerMap.put(item, n);
						headerTable.add(h);
					}
					n.setParent(currNode);
					currNode.addChild(n);
					currNode = n;
				}
			}
		}
		for(int i=0;i<headerTable.size();i++) {
			FpTreeNode n = headerTable.get(i).getHeadLink();
			long count = 0;
			while(n != null) {
				count += n.getCount();
				n = n.getNextLink();
			}
			headerTable.get(i).setTotalCount(count);
		}
		FpTree tree = new FpTree(root,headerTable); 
		return tree;
	}
	
	/* produce conditional pattern base for an item from its header entry */
	public static List<Pair>  getConditionalPatternBase(HeaderEntry h){
		List<Pair> conditionalPatternBase = new ArrayList<Pair>();
		FpTreeNode curr = h.getHeadLink();
		if(curr.getParent().getItemName() == null) {
			curr = curr.getNextLink();
		}
			
		while(curr != null) {
			FpTreeNode next = curr.getNextLink();
			long count = curr.getCount();
			curr = curr.getParent();
			StringBuilder items = new StringBuilder("");
			while(curr.getItemName() != null) {
				items.append(curr.getItemName() + " ");
				curr = curr.getParent();
			}
			curr = next;
			if(!items.toString().equals(""))
				conditionalPatternBase.add(new Pair(items.toString(),count));
		}
		return conditionalPatternBase;
	}
	
	/* get conditional pattern base for an item from header table */
	public static List<Pair>  getConditionalPatternBase(HeaderEntry h,String gListItem,boolean isItem){
		List<Pair> conditionalPatternBase = new ArrayList<Pair>();
		FpTreeNode curr = h.getHeadLink();
		if(curr.getParent().getItemName() == null) {
			curr = curr.getNextLink();
		}
			
		while(curr != null) {
			FpTreeNode next = curr.getNextLink();
			long count = curr.getCount();
			curr = curr.getParent();
			StringBuilder items = new StringBuilder("");
			boolean flag = false;
			while(curr.getItemName() != null) {
				items.append(curr.getItemName() + " ");
				if(curr.getItemName().equals(gListItem))
					flag = true;
				curr = curr.getParent();
			}
			curr = next;
			if(!items.toString().equals("") && (flag == true || isItem == true ))
				conditionalPatternBase.add(new Pair(items.toString(),count));
		}
		return conditionalPatternBase;
	}
	
	/* mine frequent patterns using fp tree */
	public static List<ListPair> mineFrequentPatterns(FpTree tree,int minSupport,String gListItem,String currItem,boolean firstPass) {
		FpTreeNode root = tree.getRoot();
		boolean onePath = false;
		List<ListPair> patterns = new ArrayList<ListPair>();
		List<ListPair> finalResult = new ArrayList<ListPair>();
		
		FpTreeNode curr = root;
		/* checking if tree has only one path */
		while(true) {
			int childCount = curr.getChildrenCount();
			if(childCount == 0) {
				onePath = true;
				break;
			}
			else if(childCount == 1) {
				List<FpTreeNode> node = curr.getChildren();
				curr = node.get(0);
			}
			else {
				onePath = false;
				break;
			}
		}
		
		if(onePath) {
			//long minCount = -1;
			List<FpTreeNode> path = root.getChildren();
			List<String> items = new ArrayList<String>();
			if(path.size() == 0)
				return finalResult;
			curr = path.get(0);
			List<Long> countList = new ArrayList<Long>();
			//System.out.println(curr.getItemName());
			while(curr != null) {
				//System.out.println(curr.getItemName());
				items.add(curr.getItemName());
				countList.add(curr.getCount());
				List<FpTreeNode> children = curr.getChildren();
				if(children.size() == 0)
					break;
				curr = children.get(0);
			}
			patterns = generateAllCombinations(items, countList,gListItem,currItem);
			/*for(int i=0;i<combinations.size();i++) {
				patterns.add(new ListPair(combinations.get(i),minCount));
			}*/
			//patterns = generateAllCombinations(items);
			//finalResult.add(items);
			finalResult.addAll(patterns);
			return finalResult;
		}
		else {
			List<HeaderEntry> headerTable = tree.getHeaderTable();
			for(int i=0;i<headerTable.size();i++) {
				//System.out.println(headerTable.get(i).getItemName());
				String itemName = headerTable.get(i).getItemName();
				if(firstPass)
					currItem = itemName;
				if(headerTable.get(i).getHeadLink().getParent().getItemName() == null) {
					if(headerTable.get(i).getHeadLink().getCount() >= minSupport) {
						if(firstPass) {
							if(itemName.equals(gListItem)) {
								ArrayList<String> item = new ArrayList<String>();
								item.add(headerTable.get(i).getItemName());
								finalResult.add(new ListPair(item,headerTable.get(i).getTotalCount()));
							}
						}
						else {
							ArrayList<String> item = new ArrayList<String>();
							item.add(headerTable.get(i).getItemName());
							finalResult.add(new ListPair(item,headerTable.get(i).getTotalCount()));
						}
					}
					if(headerTable.get(i).getHeadLink().getNextLink() == null)
						continue;
				}
				
				//List<Pair> conditionalPatternBase = getConditionalPatternBase(headerTable.get(i));
				List<Pair> conditionalPatternBase = new ArrayList<Pair>();
				if(firstPass) {
					boolean isItem = false;
					if(itemName.equals(gListItem))
						isItem = true;
					conditionalPatternBase = getConditionalPatternBase(headerTable.get(i),gListItem,isItem);
				}
					
				else
					conditionalPatternBase = getConditionalPatternBase(headerTable.get(i));
				if(conditionalPatternBase.size() == 0)
					continue;
				List<Pair> fList = makeFList(conditionalPatternBase,minSupport);
				List<String> transactions = new ArrayList<String>();
				List<Long> countList = new ArrayList<Long>();
				for(int j=0;j<conditionalPatternBase.size();j++) {
					transactions.add(conditionalPatternBase.get(j).getItem());
					countList.add(conditionalPatternBase.get(j).getCount());
				}
				FpTree newTree = constructFpTree(transactions, fList,countList);
				patterns = mineFrequentPatterns(newTree, minSupport,gListItem,currItem,false);
				for(int j=0;j<patterns.size();j++)
					patterns.get(j).addItem(itemName);
				if(firstPass) {
					if(itemName.equals(gListItem)) {
						ArrayList<String> b = new ArrayList<String>();
						b.add(headerTable.get(i).getItemName());
						patterns.add(new ListPair(b,headerTable.get(i).getTotalCount()));
					}
				}
				else {
					ArrayList<String> b = new ArrayList<String>();
					b.add(headerTable.get(i).getItemName());
					patterns.add(new ListPair(b,headerTable.get(i).getTotalCount()));
				}
				finalResult.addAll(patterns);
			}
			return finalResult;
		}
	}
	
	/* generate all combinations of a list of frequent itemsets */
	protected static List<ListPair> generateAllCombinations(List<String> items,List<Long> countList,String gListItem,String currItem){
		int n = items.size();
		long minCount = -1;
		List<ListPair> combinations = new ArrayList<ListPair>();
		//for(int i=(1<<(n-1));i<(1<<n);i++) {
		for(int i=1;i<(1<<n);i++) {
			boolean flag = false;
			ArrayList<String> list = new ArrayList<String>();
			minCount = -1;
			for(int j=1;j<=n;j++) {
				if((i & (1 << (j-1))) > 0) {
					list.add(items.get(j-1));
					if(items.get(j-1).equals(gListItem))
						flag = true;
					if(minCount == -1) {
						minCount = countList.get(j-1);
					}
					else {
						if(countList.get(j-1) < minCount)
							minCount = countList.get(j-1);
					}
				}
			}
			if(flag || gListItem.equals(currItem)) {
				//Collections.sort(list);
				//System.out.println(" -> " + list);
				combinations.add(new ListPair(list,minCount));
			}
		}
		return combinations;
	}
	
}
