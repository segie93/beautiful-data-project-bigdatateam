package edu.csula.datascience.acquisition;

/**
 * A simple model for testing
 */
public class SimpleModel {
	    private String checkin_info;
	    private String business_id;
	    private String lat;
	    private String lon;
	    private String countryCode;
	    private String categaries;
	    
		public SimpleModel(String business_id,String lat, String categaries,
				 String countryCode,String lon,String checkin_info)
		{
			this.checkin_info = checkin_info;
			this.business_id = business_id;
			this.lat = lat;
			this.lon = lon;
			this.countryCode = countryCode;
			this.categaries = categaries;
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
