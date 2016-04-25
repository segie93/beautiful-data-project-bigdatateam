package edu.csula.datascience.acquisition;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * A mock source to provide data
 */
public class MockSource implements Source<MockData> {
	 private boolean flag;

	public void setFlag(boolean flag) {
		this.flag = flag;
	}

	@Override
    public boolean hasNext() {
        return flag;
    }

    @SuppressWarnings("rawtypes")
	@Override
    public Collection<MockData> next() {
    	try {
			File json = new File(
			        ClassLoader.getSystemResource("MockData.json").toURI());
			        
			        
			        FileReader fr = new FileReader(json);
					FileWriter fw = new FileWriter(json.toString().replace("MockData.json", "FormattedMockData.json"));
					int i=0;
					Scanner scanner = new Scanner( (Readable) json );
			            while ( scanner.hasNext() ) {
			                String line = scanner.nextLine();
			                if(i==0){
			                fw.append("{\"test\":["+line);
			                i++;
			                }
			                else{
			                fw.append("\n"+","+line);
			                }
			            }
			            fw.append("]}");
			            fw.close();	
			        
			    
			
			
		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
    	
    	
    	ArrayList<MockData> list = new ArrayList<MockData>();
 		
		try {
 			File readJson = new File(
			        ClassLoader.getSystemResource("FormattedMockData.json").toURI());
 			
 			 JSONParser jsonParser = new JSONParser();
 			 JSONObject jsonObject;
			 jsonObject = (JSONObject) jsonParser.parse(new FileReader(readJson));
			 
 			 JSONArray lang= (JSONArray) jsonObject.get("test");
 			 
 			 Iterator iterator = lang.iterator();
 			  while (iterator.hasNext()) {
 			    JSONObject innerObj = (JSONObject) iterator.next();
 			      	String checkin_info = (String) innerObj.get("checkin_info");
 			      	String business_id = (String) innerObj.get("business_id");
 			     	String lat = (String) innerObj.get("lat");
 			     	String lon = (String) innerObj.get("lon");
 			     	String countryCode = (String) innerObj.get("countryCode");
 			     	String categaries = (String) innerObj.get("categaries");
 			     	
 			     	list.add(new MockData(checkin_info,business_id,lat,lon,countryCode,categaries));
 			     	
 			  }
 		} catch (ParseException | URISyntaxException | IOException e) {
 			System.out.println("Parse exception found.");
 			e.printStackTrace();
 		}	
    	
    	return list;

    }
}
