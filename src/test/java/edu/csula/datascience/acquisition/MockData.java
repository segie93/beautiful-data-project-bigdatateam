package edu.csula.datascience.acquisition;

import org.json.simple.JSONObject;

/**
 * Mock raw data
 */
public class MockData {
    private String checkin_info;
    private String business_id;
    private String lat;
    private String lon;
    private String countryCode;
    private String categaries;
    
    public MockData(JSONObject obj) {
	
		this.checkin_info = (String) obj.get("checkin_info");
		this.business_id = (String) obj.get("business_id");
		this.lat = (String) obj.get("lat");
		this.lon = (String) obj.get("lon");
		this.countryCode = (String) obj.get("countryCode");
		this.categaries = (String) obj.get("categaries");
	}
	
	

	public String getCheckin_info() {
		return checkin_info;
	}
	
	public String getBusiness_id() {
		return business_id;
	}
	public String getLat() {
		return lat;
	}
	public String getLon() {
		return lon;
	}
	public String getCountryCode() {
		return countryCode;
	}
	public String getCategaries() {
		return categaries;
	}
     
}
