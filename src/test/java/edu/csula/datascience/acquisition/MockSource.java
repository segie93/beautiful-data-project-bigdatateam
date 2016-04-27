package edu.csula.datascience.acquisition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.json.simple.JSONObject;

import com.google.common.collect.Lists;

public class MockSource implements Source<MockData>{
	
	boolean flag = true;
	
	ArrayList<MockData> list = new ArrayList<MockData>(); 

	public void setFlag(boolean flag) {
		this.flag = flag;
	}

	@Override
	public boolean hasNext() {
		
		return flag;
	}

	@Override
	public Collection<MockData> next() {

		/*String json1 = "{'business_id':'1''lon':'-74.003139','categaries':'Jazz-Club','countryCode':'US','lat':'40.733596','checkin_info': '9-5:1'}";
		String json2= "{'business_id':'2','lon':'-73.975734','categaries':'Gym','countryCode':'US','lat':'40.758102','checkin_info': '16-2:1'}";
		String json3= "{'business_id':'3','lon':'-74.003755','categaries':'Indian-Restaurant','countryCode':'US','lat':'40.732456','checkin_info': '5-5:2'}";*/
		
		String[] key ={"business_id","lon","categaries","countryCode","lat","checkin_info"}; 
		String[] value = {"1","-74.003139","Jazz-Club","US","40.733596","9-5:1"};
		String[] value2 = {"2","-74.003139","Gym","US","40.734396","5-3:4"};
		String[] value3 = {"3","-74.003139","Indian-Restaurent","US","40.734396","5-3:4"};
		
		HashMap<String, String> map = new HashMap<String, String>();
		
		for(int i=0;i<key.length;i++){
			map.put(key[i], value[i]);
		}
		JSONObject obj = new JSONObject();
		obj.putAll(map);
		
		HashMap<String, String> map2 = new HashMap<String, String>();
		for(int i=0;i<key.length;i++){
			map.put(key[i], value2[i]);
		}
		JSONObject obj2 = new JSONObject();
		obj2.putAll(map2);
		
		HashMap<String, String> map3 = new HashMap<String, String>();
		for(int i=0;i<key.length;i++){
			map.put(key[i], value3[i]);
		}
		JSONObject obj3 = new JSONObject();
		obj3.putAll(map3);
		
		
		
		list.add(new MockData(obj));
		list.add(new MockData(obj2));
		list.add(new MockData(obj3));
		
		return list;
		/*return Lists.newArrayList(
				(new MockData3(obj)),
				new MockData3(obj2),
				new MockData3(obj3));*/
	}


}
