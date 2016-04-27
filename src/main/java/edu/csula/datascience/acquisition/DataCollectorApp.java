package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

/**
 * A simple example of using Twitter
 */
public class DataCollectorApp {
    public static void main(String[] args) throws FileNotFoundException, IOException {
    	boolean flag = false;
    	
    	String filePath = "S:\\Yelp Data";
    	File folder = new File("S:\\Yelp Data");
    	File[] listOfFiles = folder.listFiles();
    	
    	if(listOfFiles.length>0)
    		flag=true;
    	
    	DataSource source = new DataSource(flag);
    	DataCollector collector = new DataCollector();
    	
    	while(source.hasNext()){
    		System.out.println("Reading the files!!1");
    		Collection<String> readFilePath = source.next();
    		System.out.println("All formatted data path"+readFilePath);
    		source.setFlag(false);
    		collector.save(readFilePath);
    	}	     
    	System.out.println("Done!!!");
    }
}