package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import aggregator.AggregatorMapper;
import aggregator.AggregatorReducer;
import aggregator2.Aggregator2Mapper;
import aggregator2.Aggregator2Reducer;
import parallelCounting.ParallelCountingMapper;
import parallelCounting.ParallelCountingReducer;
import parallelFpGrowth.ParallelFpMapper;
import parallelFpGrowth.ParallelFpReducer;
import utils.Pair;
import utils.SortByCount;

public class FpGrowth {
	
	private final static int noOfGroups = 1000;
	
	protected static void runFpGrowth(String inputFile,String outputFile,int minSupport,int maxPatterns) throws IOException, ClassNotFoundException , InterruptedException{
		Configuration conf = new Configuration();
		conf.set("outputFile", outputFile);
		conf.set("noOfGroups", String.valueOf(noOfGroups));
		conf.set("maxPatterns", String.valueOf(maxPatterns));
		startParallelCounting(conf,inputFile,outputFile,minSupport);
		List<Pair> fList = getFList(outputFile);
		saveFlist(fList,conf,outputFile);
		startParallelFpGrowth(conf,inputFile,outputFile);
		startAggregation(conf,outputFile);
		startFinalAggregation(conf,outputFile);
	}
	
	/* save F-list to hdfs in a file */
	protected static void saveFlist(List<Pair> fList,Configuration conf,String outputFile) throws IOException {
		String destination = outputFile + "/fList";
		FileSystem fs = FileSystem.get(URI.create(destination), conf);
		OutputStream out = fs.create(new Path(destination));
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter(out) );
		for(Pair itemAndCount: fList) {
			br.write(itemAndCount.getItem() + " " + itemAndCount.getCount() + "\n");
		}
		br.close();
		fs.close();
	}
	
	/* read F-list from hdfs */
	public static List<Pair> readFList(Configuration conf) throws IOException{
		Path p = new Path(conf.get("outputFile") + "/fList");
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line = br.readLine();
        List<Pair> fList = new ArrayList<Pair>();
        while (line != null){
        	StringTokenizer s1 = new StringTokenizer(line);
        	String item = s1.nextToken();
        	long count  = Long.parseLong(s1.nextToken());
        	fList.add(new Pair(item,count));
        	line = br.readLine();
        }
        return fList;
		/*Path[] files = DistributedCache.getLocalCacheFiles(conf);
        FileSystem fs = FileSystem.getLocal(conf);
        Path fListLocalPath = fs.makeQualified(files[0]);
        URI[] filesURIs = DistributedCache.getCacheFiles(conf);
        fListLocalPath = new Path(filesURIs[0].getPath());*/
        /*for (Pair itemAndCount : new SequenceFileIterable<Text, LongWritable>(fListLocalPath, true, conf)) {
            fList.add(new Pair(itemAndCount.getItem().toString(),itemAndCount.getCount()));
        }
        return fList;*/
	}
	
	protected static void startParallelFpGrowth(Configuration conf,String inputFile,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		Job job = new Job(conf,"parallelFpGrowth");
		job.setJarByClass(FpGrowth.class);
		job.setMapperClass(ParallelFpMapper.class);
	   	job.setReducerClass(ParallelFpReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	job.setMapOutputKeyClass(IntWritable.class);
	   	job.setMapOutputValueClass(Text.class);
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile+"1"));
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	/* reads items and count from HDFS after 1st map reduce, sorts it according to count(desc) and returns it*/
	protected static List<Pair> getFList(String outputFile) {
		try {
			Path p = new Path(outputFile + "/part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line;
            line = br.readLine();
            List<Pair> fList = new ArrayList<Pair>();
            while (line != null){
            	StringTokenizer s1 = new StringTokenizer(line);
            	String item = s1.nextToken();
            	long count  = Long.parseLong(s1.nextToken());
            	fList.add(new Pair(item,count));
            	line = br.readLine();
            }
            Collections.sort(fList,new SortByCount());
            return fList;
		}
		catch(Exception e) {
			System.out.println(e);
			return null;
		}
	}
	
	/* get count of all items in transactions */
	protected static void startParallelCounting(Configuration conf,String inputFile,String outputFile,int minSupport) throws IOException,InterruptedException,ClassNotFoundException{
		conf.set("minSupport", String.valueOf(minSupport));
		Job job = new Job(conf, "countItems");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(ParallelCountingMapper.class);
	   	job.setReducerClass(ParallelCountingReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	/* aggregation of patterns so that only unique patterns are present */
	protected static void startAggregation(Configuration conf,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		Job job = new Job(conf,"aggregating");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(Aggregator2Mapper.class);
		job.setReducerClass(Aggregator2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(outputFile+"1/part-r-00000")); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile+"final"));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	/* final aggregation where all unique frequent itemsets have their frequent patterns */
	protected static void startFinalAggregation(Configuration conf,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		Job job = new Job(conf,"finalAggregation");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(AggregatorMapper.class);
		job.setReducerClass(AggregatorReducer.class);
		job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(Text.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(outputFile+"final"+"/part-r-00000")); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile+"final1"));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
}
