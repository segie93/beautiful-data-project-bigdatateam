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
import java.util.Iterator;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * A quick elastic search example app
 *
 * It will parse the csv file from the resource folder under main and send these
 * data to elastic search instance running locally
 *
 * After that we will be using elastic search to do full text search
 *
 * gradle command to run this app `gradle esExample`
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
          "full_address": {
            "type": "string",
            "index": "not_analyzed"
          },
          "state": {
            "type": "string",
            "index": "not_analyzed"
          }
        }
      }
    }
}
 */
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

        try {
	
	    		JSONObject jsonObject = (JSONObject) jsonParser.parse(new FileReader("S:\\formated-data\\formatted-yelp_academic_dataset_business.json"));;
	    		JSONArray jsonarray = (JSONArray)jsonObject.get("test");
	    		
   			 //System.out.println(jsonarray);
   			 
   			 Iterator i = jsonarray.iterator();
   			
   			  while (i.hasNext()) {
   				JSONObject innerObj = (JSONObject) i.next();

   			    String business_id = (String) innerObj.get("business_id");
	    		
	    		String full_address = (String) innerObj.get("full_address");
	    		
	    		String city = (String) innerObj.get("city");
	    		
	    		String state = (String) innerObj.get("state");  
   	
	    		int year = 2016;
	    		YelpData temp = new YelpData(business_id,full_address,city,state,year);
	    		 bulkProcessor.add(new IndexRequest(indexName, typeName)
                 .source(gson.toJson(temp))
             );
	    		 
   			  }
   			  System.out.println("Done!!!!");
	    		 
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	} catch (ParseException e) {
	    		e.printStackTrace();
	    	}
	    	
        
      

        
/*        try {
            // after reading the csv file, we will use CSVParser to parse through
            // the csv files
            CSVParser parser = CSVParser.parse(
                csv,
                Charset.defaultCharset(),
                CSVFormat.EXCEL.withHeader()
            );

            // for each record, we will insert data into Elastic Search
            parser.forEach(record -> {
                // cleaning up dirty data which doesn't have time or temperature
                if (
                    !record.get("dt").isEmpty() &&
                    !record.get("AverageTemperature").isEmpty()
                ) {
                    Temperature temp = new Temperature(
                        record.get("dt"),
                        Double.valueOf(record.get("AverageTemperature")),
                        record.get("State"),
                        record.get("Country")
                    );

                    bulkProcessor.add(new IndexRequest(indexName, typeName)
                        .source(gson.toJson(temp))
                    );
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }*/


    }

    static class YelpData {
        String business_id;
        String full_address;
        String city;
        String state;
        Integer date;

        public YelpData(String business_id, String full_address, String city, String state, Integer date) {
            this.business_id = business_id;
            this.full_address = full_address;
            this.city = city;
            this.state = state;
            this.date = date;
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
