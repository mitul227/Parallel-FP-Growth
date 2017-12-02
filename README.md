# Parallel-FP-Growth
Implementation of Parallel FP Growth Algorithm on a Hadoop Cluster for mining frequent itemsets from a large transactional Database using Map Reduce Programming

More can be found at - https://en.wikibooks.org/wiki/Data_Mining_Algorithms_In_R/Frequent_Pattern_Mining/The_FP-Growth_Algorithm

# USE
Consider your transactional database consists of huge amount of transactions -

Eg -
1) Milk,Butter,Bread,Chicken.....
2) Chair,Table,Cloth....

and so on....

Output - 

Milk - ([Milk,Butter,Bread]:5),([Milk,Butter,Cornflakes]:7),([Milk,Flour,Chicken,Cheese,Bread]:11) 

Bread - ....

and so on...

FP algorithm is used to find frequent itemsets along with their count for every item in this database which is considered to be frequent (this is specified by a minimum support count)

Parallelized version of this algorithm is implemented in Hadoop so that multiple computers in the hadoop cluster can process data parallely in a large database

Using these frequent itemsets for an item we can also design recommendation system when we buy an item like in e-Commerce Websites

