package edu.csula.datascience.acquisition;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;

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

/**
 * An example of Collector implementation using Twitter4j with MongoDB Java driver
 */
public class DataCollector implements Collector<Status, Status> {
    MongoClient mongoClient;
    MongoDatabase database;
    MongoCollection<Document> collection;
   /* public DataCollector() {
        // establish database connection to MongoDB
        mongoClient = new MongoClient();
        // select `bd-example` as testing database
        database = mongoClient.getDatabase("hw2bigdata");

        // select collection by name `tweets`
        collection = database.getCollection("hw2dataset");
    }*/
    
    @Override
    public Collection<Status> mungee(Collection<Status> src) {
        return src;
    }

    @Override
    public void save(Collection<String> data) {
    	int i=data.size();
    	while(data.iterator().hasNext()){
    		try {
				System.out.println("FOUND!!!!");
				 JSONParser jsonParser = new JSONParser();
				 JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader(data.iterator().next()));
				 System.out.println("done");
				 JSONArray lang= (JSONArray) jsonObject.get("test");
				 
				 DBObject dbObject = (DBObject)JSON.parse(lang.get(0).toString());
	    			List<DBObject> list = new ArrayList<>();
	    			list.add(dbObject);	
	    			//collection.insertMany(list);
	    			new MongoClient().getDB("hw2bigdata").getCollection("hw2dataset").insert(list);
	    			i--;
	    			if(i==0)
	    				break;
	    			
			} catch (ParseException | IOException e) {
				System.out.println("Parse exception found.");
				e.printStackTrace();
			}
    	}
    }
}