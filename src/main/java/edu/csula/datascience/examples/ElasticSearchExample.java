package edu.csula.datascience.examples;

import com.google.gson.Gson;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * A quick elastic search example to insert data
 *
 * It will parse the json file and send these
 * data to elastic search instance running locally
 *
 * After that we will be using elastic search to do full text search
 *
 * gradle command to run this app `gradle esHomework`
 */

/*
PUT /yelp-data
 {
    "mappings": {
      "shweet": {
        "properties": {
          "business_id": {
            "type": "string",
            "index": "not_analyzed"
          },
          "city": {
            "type": "string",
            "index": "not_analyzed"
          },
          "date": {
            "type": "date",
            "format": "strict_date_optional_time||epoch_millis"
          },
          "day": {
          "type": "string",
          "index": "not_analyzed"
        },
        "categories": {
          "type": "string",
          "index": "not_analyzed"
        }
        }
      }
    }
}*/

public class ElasticSearchExample {
    private final static String indexName = "yelp-data";
    private final static String typeName = "shweet";

    public static void main(String[] args) throws URISyntaxException, IOException {
        Node node = nodeBuilder().settings(Settings.builder()
            .put("cluster.name", "Segie")
            .put("path.home", "elasticsearch-data")).node();
        Client client = node.client();

        /*String filePath = "S:\\Yelp Data";
    	File folder = new File("S:\\Yelp Data");
    	File[] listOfFiles = folder.listFiles();
    	*/	 
    	 
        // create bulk processor
        BulkProcessor bulkProcessor = BulkProcessor.builder(
            client,
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId,
                                       BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      BulkResponse response) {
                }

                @Override
                public void afterBulk(long executionId,
                                      BulkRequest request,
                                      Throwable failure) {
                    System.out.println("Facing error while importing data to elastic search");
                    failure.printStackTrace();
                }
            })
            .setBulkActions(10000)
            .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.GB))
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .setConcurrentRequests(1)
            .setBackoffPolicy(
                BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
            .build();

        // Gson library for sending json to elastic search
        Gson gson = new Gson();

        JSONParser jsonParser = new JSONParser();

        HashMap<String, String> cities = new HashMap<String, String>();
        HashMap<String, String> cat = new HashMap<String, String>();
        
        try {
	
	    		JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_business.json"));;
	    		JSONArray jsonarray = (JSONArray)jsonObject.get("test");
	    		
   			 //System.out.println(jsonarray);
   			 
   			 Iterator i = jsonarray.iterator();
   			
   			  while (i.hasNext()) {
   				JSONObject innerObj = (JSONObject) i.next();

   			    String business_id = (String) innerObj.get("business_id");
	    		
	    		//String full_address = (String) innerObj.get("full_address");
	    		
	    		String city = (String) innerObj.get("city");
	    		
	    		cities.put(business_id, city);
	    		/*String state = (String) innerObj.get("state"); 
	    		
	    		double lat = (double) innerObj.get("latitude");
	    		
	    		double lon = (double) innerObj.get("longitude");
	    		
	    		long review_count = (long) innerObj.get("review_count");
	    		//System.out.println("--->>"+review_count);
	    		Double stars = (Double) innerObj.get("stars");
	    		
	    		String location = lat+","+lon;
	    		//System.out.println(location);
	    		int year = 2016;*/
	    		
	    		JSONArray categories = (JSONArray) innerObj.get("categories");
	    		String catt ="";
	    		if(categories.size()>0){
	    			catt = categories.get(categories.size()-1).toString();
	    			/*for(int k=0;k<categories.size();k++){
	    		    
	    		    catt += categories.get(k).toString();
	    		    YelpData temp = new YelpData(business_id,full_address,city,state,year,location,
		    				review_count,stars,cat);
	    		    //System.out.println(gson.toJson(temp));
	    		    bulkProcessor.add(new IndexRequest(indexName, typeName)
	                 .source(gson.toJson(temp)) 
	                 );
	    			} */
	    		}
	    		else{
	    			/*YelpData temp = new YelpData(business_id,full_address,city,state,year,location,
		    				review_count,stars,"");
	    			//System.out.println(gson.toJson(temp));
	    		    bulkProcessor.add(new IndexRequest(indexName, typeName)
	                 .source(gson.toJson(temp)) 
	                 );*/
	    		}
	    		/*YelpData temp = new YelpData(business_id,full_address,city,state,year,location,
	    				review_count,stars);
	    		 bulkProcessor.add(new IndexRequest(indexName, typeName)
                 .source(gson.toJson(temp))
             );*/
	    		 //System.out.println(catt+"--------------------------------------");
	    		cat.put(business_id, catt);
   			  }
   			  System.out.println("Done!!!!");
	    		 
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	} catch (ParseException e) {
	    		e.printStackTrace();
	    	}
        
        
        try {
        	
    		JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_checkin.json"));;
    		JSONArray jsonarray = (JSONArray)jsonObject.get("test");
    		
			 //System.out.println(jsonarray);
			 
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
    		//System.out.println("---"+gson.toJson(temp));
    		 bulkProcessor.add(new IndexRequest(indexName, typeName)
             .source(gson.toJson(temp))
    		);
			 }
			  }
			  System.out.println("Done!!!!");
    		 
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	} catch (ParseException e) {
    		e.printStackTrace();
    	}
        
        
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
