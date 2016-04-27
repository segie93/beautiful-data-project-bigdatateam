package edu.csula.datascience.acquisition;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.json.JSONObject;


public class DataSource implements Source<String> {
    private boolean flag;
    private String[] keys; 
    
    public void setKeys(String[] keys) {
		this.keys = keys;
	}

	public DataSource(Boolean flag) {
        this.flag=flag;
    }

    @Override
    public boolean hasNext() {
    	return flag;   	
    }

    public void setFlag(boolean flag) {
		this.flag = flag;
	}

	@Override
    public Collection<String> next() {
    	 List<String> list = Lists.newArrayList();
         
    	 File folder = new File("S:\\Yelp Data");
     	 File[] listOfFiles = folder.listFiles();
     	 System.out.println("Formatting the files...");
     	 for (int i = 0; i < listOfFiles.length; i++) {
     		//System.out.println(listOfFiles[i].getName().toString());
            
     		 if (listOfFiles[i].isFile()) {
         	try {
         		if(listOfFiles[i].getName().toString().contains(".json")){
				formatdata("S:\\Yelp Data\\"+listOfFiles[i].getName().toString(),"S:\\formated-data\\"+ "formatted-"+listOfFiles[i].getName().toString());
				list.add("S:\\formated-data\\"+ "formatted-"+listOfFiles[i].getName().toString());
         		}
         		if(listOfFiles[i].getName().toString().contains(".txt")){
         			if(listOfFiles[i].getName().toString().contains("Cities.txt")){
         				String[] cityKyes = { "city", "lat", "lon", "countryCode", "countryName", "cityType" };	
         				setKeys(cityKyes);
         				formattxtdata("S:\\Yelp Data\\"+listOfFiles[i].getName().toString()
         						,"S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         				list.add("S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         			}
         			if(listOfFiles[i].getName().toString().contains("Checkins.txt")){
         				String[] checkinKeys = { "userId", "venueId", "UTCTime", "timeZone" };	
         				setKeys(checkinKeys);
         				formattxtdata("S:\\Yelp Data\\"+listOfFiles[i].getName().toString()
         						,"S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         				list.add("S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         			}
         			if(listOfFiles[i].getName().toString().contains("POIs.txt")){
         				String[] poiKeys = { "venue", "lat", "lon", "category", "countryCode" };
         				setKeys(poiKeys);
         				formattxtdata("S:\\Yelp Data\\"+listOfFiles[i].getName().toString()
         						,"S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         				list.add("S:\\formated-data\\"+ "formatted-txt-"+listOfFiles[i].getName().toString().replace(".txt", ".json"));
         			}
         			//source.save(to);
         		}
         	} catch (IOException e) {
				e.printStackTrace();
			}
             //System.out.println(listOfFiles[i].getName());
           }
         }
    	 
         return list;
    }

	@SuppressWarnings("unused")
	private void fixtxtdata(String rEAD_FILE, String wRITE_FILE) {
		try {
			FileWriter file = new FileWriter(wRITE_FILE);
			file.write("{\"test\":[\n");
			FileWriter pois = new FileWriter("S:\\POIs.json");
			JSONObject lineObject = new JSONObject();
			String[] elements;
		
			BufferedReader br = new BufferedReader(new FileReader(rEAD_FILE));
			String line = br.readLine();
		
			int i = 0;
			while (line != null && line.contains("US")) {
				    pois.write(line+"\n");
					elements = line.split("\\s+");
					lineObject = getJson(keys, elements);
					file.write(lineObject.toString());
					if (i % 1000000 == 0) {
						System.out.println((i/1000) + "k records");
					}
					i++;
					line = br.readLine();
					if (line != null && line.trim() != "") {
						file.write(",\n");
					}
			}
			
			file.write("\n]}");
			file.close();
			pois.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void formatdata(String rEAD_FILE, String wRITE_FILE) throws IOException {
		//System.out.println(rEAD_FILE+"---"+wRITE_FILE);
		FileReader file = new FileReader(rEAD_FILE);
		FileWriter fw = new FileWriter(wRITE_FILE);
		int i=0;
		Scanner scanner = new Scanner( (Readable) file );
            while ( scanner.hasNext() ) {
                String line = scanner.nextLine();
               //System.out.println(line+"\n");
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
            
	}
	
	private void formattxtdata(String rEAD_FILE, String wRITE_FILE){
			try {
				FileWriter file = new FileWriter(wRITE_FILE);
				file.write("{\"test\":[\n");
			
				JSONObject lineObject = new JSONObject();
				String[] elements;
			
				BufferedReader br = new BufferedReader(new FileReader(rEAD_FILE));
				String line = br.readLine();
			
				int i = 0;
				while (line != null) {
						elements = line.split("\\s+");
						lineObject = getJson(keys, elements);
						file.write(lineObject.toString());
						i++;
						line = br.readLine();
						if (line != null && line.trim() != "") {
							file.write(",\n");
						}
				}
				
				file.write("\n]}");
				file.close();
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

	private JSONObject getJson(String[] keys, String[] values) {
		Map<String, String> map = new HashMap<>();
		JSONObject object =  new JSONObject();
		for (int i = 0; i < keys.length; i++) {
			object.put(keys[i], values[i]);
		//	map.put(keys[i], values[i]);
			
		}
		return object;
	}
}