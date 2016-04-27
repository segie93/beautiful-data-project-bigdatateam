package edu.csula.datascience.acquisition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class MockCollector implements Collector<SimpleModel, MockData>{
	
	@Override
	public Collection<SimpleModel> mungee(Collection<MockData> src) {
		
		ArrayList<SimpleModel> list = new ArrayList<SimpleModel>();
		int i = src.size();
		
    	while(src.iterator().hasNext()){
    		MockData line = src.iterator().next();
    		list.add(new SimpleModel(line.getBusiness_id(),line.getLat(),line.getCategaries(),
					line.getCountryCode(),line.getLon(),line.getCheckin_info()));
    		i--;
    		if(i==0)
    			break;
    	}
    	return list;
	}

	@Override
	public void save(Collection<String> data) {

	}


}
