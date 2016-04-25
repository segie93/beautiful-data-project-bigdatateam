package edu.csula.datascience.acquisition;

/**
 * A simple model for testing
 */
public class SimpleModel {
	 private final String checkin_info;
	    private final String business_id;
	    private final String lat;
	    private final String lon;
	    private final String countryCode;
	    private final String categaries;
	   
		public SimpleModel(String checkin_info, String business_id,
				String lat, String lon, String countryCode, String categaries)
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
