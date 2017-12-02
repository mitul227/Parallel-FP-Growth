package main;

import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
		// TODO Auto-generated method stub
		String inputFile = args[0];
		String outputFile = args[1];
		int minimumSupport = Integer.parseInt(args[2]);
		int maxPatterns = Integer.parseInt(args[3]);
		long startTime = System.currentTimeMillis();
		FpGrowth.runFpGrowth(inputFile,outputFile,minimumSupport,maxPatterns);
		long endTime = System.currentTimeMillis();
  		long totalTime = endTime-startTime;
  		System.out.println("time - " + totalTime);
	}

}
