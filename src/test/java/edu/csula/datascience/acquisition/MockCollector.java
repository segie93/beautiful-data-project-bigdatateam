package edu.csula.datascience.acquisition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * A mock implementation of collector for testing
 */
public class MockCollector implements Collector<SimpleModel, MockData> {
    @Override
    public Collection<SimpleModel> mungee(Collection<MockData> src) {
        // in your example, you might need to check src.hasNext() first
    	
    	ArrayList<SimpleModel> list = new ArrayList<SimpleModel>();

		for (Iterator<MockData> iterator = src.iterator(); iterator.hasNext();) {
			MockData line = iterator.next();
			if(line.getBusiness_id() !=null && line.getCategaries() !=null && line.getCheckin_info() !=null 
					&& line.getCountryCode() !=null && line.getLat()!=null && line.getLon()!=null){
				list.add(new SimpleModel(line.getBusiness_id(),line.getCategaries(),line.getCheckin_info(),
						line.getCountryCode(),line.getLat(),line.getLon()));
			}

		}
    	
        return list;

    }

    @Override
    public void save(Collection<SimpleModel> data) {
    }
}
