package edu.csula.datascience.acquisition;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import twitter4j.Status;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An example of Collector implementation using Twitter4j with MongoDB Java driver
 */
public class DataCollector implements Collector<Status, Status> {
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;
    public DataCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient();
        // select `bd-example` as testing database
        database = mongoClient.getDatabase("hw2bigdata");

        // select collection by name `tweets`
        collection = database.getCollection("hw2dataset");
    }
    
    @Override
    public Collection<Status> mungee(Collection<Status> src) {
        return src;
    }

    @Override
    public void save(Collection<Status> data) {
        
        
       // collection.insertMany(documents);
    }
	public Collection<String> cleanData(/*Collection<String> readFilePath*/) throws FileNotFoundException, IOException {
		
		String filePath = "S:\\Yelp Data";
    	File folder = new File("S:\\Yelp Data");
    	File[] listOfFiles = folder.listFiles();
    	 for (int i = 0; i < listOfFiles.length; i++) {
	/*	Iterator i = readFilePath.iterator();
		while(i.hasNext()){*/
			ArrayList<String> listofnames = new ArrayList<String>();
			//System.out.println(i.next()+"\n");
			//if(listOfFiles[i].toString().contains("POIs")){
				
				/*try {
					System.out.println("FOUND!!!!");
					 JSONParser jsonParser = new JSONParser();
					 JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-txt-dataset_TIST2015_POIs.json"));
					 System.out.println("done");
					 JSONArray lang= (JSONArray) jsonObject.get("test");
					 
					 Iterator iterator = lang.iterator();
					 System.out.println("inside---");
					 while (iterator.hasNext()) {
						JSONObject innerObj = (JSONObject) iterator.next();
					     
					      String GroupName = (String) innerObj.get("countryCode");
					      System.out.println("--"+GroupName);
					      listofnames.add(GroupName);
					  }
					  System.err.println(listofnames);
					  
				} catch (ParseException e) {
					System.out.println("Parse exception found.");
					e.printStackTrace();
				}*/
			
			/**/
		
    	 }
		return null;
	}
}
