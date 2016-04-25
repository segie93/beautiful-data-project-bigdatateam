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
    		Collection<String> readFilePath = source.next();
    		//System.out.println("All formatted data path"+readFilePath);
    		source.setFlag(false);
    		//Collection<String> sa = collector.cleanData();
    		//collector.save(readFilePath);
    	}	     
    	System.out.println("Done!!!");

    	/*
        for (int i = 0; i < listOfFiles.length; i++) {
          if (listOfFiles[i].isFile()) {
        	formatdata("S:\\Yelp Data\\"+listOfFiles[i].getName().toString(),"S:\\formated-data\\"+ "formatted-"+listOfFiles[i].getName().toString());
            //System.out.println(listOfFiles[i].getName());
          }
        }*/
    	/*ArrayList<String> listofnames = new ArrayList<String>();
		
 		try {
 			
 			 JSONParser jsonParser = new JSONParser();
 			 JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_user.json"));
 			 
 			 JSONArray lang= (JSONArray) jsonObject.get("test");
 			 
 			 Iterator iterator = lang.iterator();
 			  while (iterator.hasNext()) {
 			    JSONObject innerObj = (JSONObject) iterator.next();
 			      String GroupName = (String) innerObj.get("name");
 			      listofnames.add(GroupName);
 			  }
 		} catch (ParseException e) {
 			System.out.println("Parse exception found.");
 			e.printStackTrace();
 		}
 		System.out.println("done--"+listofnames);//);
*/    
    }

}