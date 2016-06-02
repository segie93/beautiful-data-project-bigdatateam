package edu.csula.datascience.examples;

import com.google.common.collect.Lists;

import edu.csula.datascience.examples.ElasticSearchExample.YelpData;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

	/**
	 * A quick example app to send data to elastic search on AWS
	 */
	public class JestYelpdataInsert {
	    public static void main(String[] args) throws URISyntaxException, IOException {
	        String indexName = "biggy-data";
	        String typeName = "yelp";
	        String awsAddress = "https://search-bigdata-ui3hg3arrxjln6bjnfhtpa6d5y.us-west-2.es.amazonaws.com/";
	        JestClientFactory factory = new JestClientFactory();
	        factory.setHttpClientConfig(new HttpClientConfig
	            .Builder(awsAddress)
	            .multiThreaded(true)
	            .build());
	        JestClient client = factory.getObject();


	        JSONParser jsonParser = new JSONParser();

	        HashMap<String, String> cities = new HashMap<String, String>();
	        HashMap<String, String> cat = new HashMap<String, String>();
	        Collection<YelpData> yelpData = Lists.newArrayList();
	        
            
	        try {
		
		    		JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_business.json"));;
		    		JSONArray jsonarray = (JSONArray)jsonObject.get("test");
		    		
		   		    Iterator i = jsonarray.iterator();
	   			
	   			  while (i.hasNext()) {
	   				JSONObject innerObj = (JSONObject) i.next();

	   			    String business_id = (String) innerObj.get("business_id");
		    		String city = (String) innerObj.get("city");
		    		
		    		cities.put(business_id, city);
		    		
		    		JSONArray categories = (JSONArray) innerObj.get("categories");
		    		String catt ="";
		    		if(categories.size()>0){
		    			//extract the categories from json array 
		    			catt = categories.get(categories.size()-1).toString();
		    			
		    		}
		    		else{
		    			catt="";
		    		}
		    		cat.put(business_id, catt);
	   			  }
	   			  System.out.println("Reading the 1st file Done!!!!");
		    		 
		    	} catch (FileNotFoundException e) {
		    		e.printStackTrace();
		    	} catch (IOException e) {
		    		e.printStackTrace();
		    	} catch (ParseException e) {
		    		e.printStackTrace();
		    	}
	        
	        int count =0;
	        try {
	        	
	    		JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_checkin.json"));;
	    		JSONArray jsonarray = (JSONArray)jsonObject.get("test");
	    		
	    		Iterator i = jsonarray.iterator();
				 int year = 2016;
				 while (i.hasNext()) {
					
					 JSONObject innerObj = (JSONObject) i.next();

				     String business_id = (String) innerObj.get("business_id");
				     JSONObject checkin_info = (JSONObject) innerObj.get("checkin_info");
				     
				 for(Iterator iterator = checkin_info.keySet().iterator(); iterator.hasNext();) {
				     String key = (String) iterator.next();
				     String[] ck = key.split("-");
				     String day = getDay(ck[1]);
				     Long checkin_count = (Long)checkin_info.get(key);
				     String city = cities.get(business_id);
				     String categories = cat.get(business_id);
				
	    		YelpData temp = new YelpData(business_id,year,day,Integer.parseInt(ck[0].toString()),
	    				checkin_count,city,categories);
	    		
	    		if (count < 500) {
                    yelpData.add(temp);
                    count ++;
                } else {
                	Collection<BulkableAction> actions = Lists.newArrayList();
                	
                	yelpData.stream()
                    .forEach(tmp -> {
                        actions.add(new Index.Builder(tmp).build());
                    });

                	Bulk.Builder bulk = new Bulk.Builder()
		            .defaultIndex(indexName)
		            .defaultType(typeName)
		            .addAction(actions);
		            client.execute(bulk.build());
		            count = 0;
                    yelpData = Lists.newArrayList();
                    System.out.println("500 Records inserted");
				 	}
				}
			} 
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	} catch (ParseException e) {
	    		e.printStackTrace();
	    	}
	        //to add data left at the end
	        Collection<BulkableAction> actions = Lists.newArrayList();
            yelpData.stream()
                .forEach(tmp -> {
                    actions.add(new Index.Builder(tmp).build());
                });
            Bulk.Builder bulk = new Bulk.Builder()
                .defaultIndex(indexName)
                .defaultType(typeName)
                .addAction(actions);
            client.execute(bulk.build());

	        System.out.println("We are done! Yay!");
	    }
	    
	    public static String getDay(String key){
	    	HashMap<Integer, String> days = new HashMap<Integer, String>();
	    	days.put(0, "Sunday");
	    	days.put(1, "Monday");
	    	days.put(2, "Tusday");
	    	days.put(3, "Wednesday");
	    	days.put(4, "Thursday");
	    	days.put(5, "Friday");
	    	days.put(6, "Saturday");
	    	
	    	
	    	return days.get(Integer.parseInt(key));
	    }

	    static class YelpData {
	        String business_id;
	        String full_address;
	        String city;
	        String state;
	        Integer date;
	        String location;
	        Long review_count;
	        Double stars; 
	        String categories;

	        public YelpData(String business_id, String full_address, String city, String state, Integer date,
	        		String location,Long review_count,Double stars,String categories) {
	            this.business_id = business_id;
	            this.full_address = full_address;
	            this.city = city;
	            this.state = state;
	            this.date = date;
	            this.location=location;
	            this.review_count=review_count;
	            this.stars=stars;
	            this.categories=categories;
	        }
	        
	        Integer time;
	        String day;
	        Long checkin_count;
	        
	        public YelpData(String business_id,Integer date,String day,Integer time,Long checkin_count,
	        		String city,String categories) {
	            this.business_id = business_id;
	            this.date=date;
	            this.day=day;
	            this.time=time;
	            this.checkin_count=checkin_count;
	            this.city=city;
	            this.categories=categories;
	     
	        }
	        

			public Long getReview_count() {
				return review_count;
			}


			public String getLocation() {
				return location;
			}


			public Double getStars() {
				return stars;
			}


			public String getBusiness_id() {
				return business_id;
			}

			public String getFull_address() {
				return full_address;
			}

			public Integer getDate() {
				return date;
			}

			public String getCity() {
				return city;
			}

			public String getState() {
				return state;
			}

	    }
 }
	
