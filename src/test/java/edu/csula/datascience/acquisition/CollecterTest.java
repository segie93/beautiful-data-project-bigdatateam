package edu.csula.datascience.acquisition;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public class CollecterTest {
	
	private Collector<SimpleModel, MockData> collector;
    private Source<MockData> source;

    @Before
    public void setup() {
        collector = new MockCollector();
        source = new MockSource();
    }

    @Test
    public void mungee() throws Exception {
        List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
        List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("1","-74.003139","Jazz-Club","US","40.733596","9-5:1"),
            new SimpleModel("2","-73.975734","Gym","US","40.758102","16-2:1"),
            new SimpleModel("3","-74.003755","Indian-Restaurant","US","40.732456","4-6:1")
        );
        
        //Size Test
       	Assert.assertEquals(list.size(),3);
      
        //US only Data Test!!!
        for (int i = 0; i < 3; i ++) {
            Assert.assertEquals(list.get(i).getCountryCode(),expectedList.get(i).getCountryCode());
        }
        
        //Check-in info is not null
        for (int i = 0; i < 3; i ++) {
        	Assert.assertFalse(list.get(i).getCheckin_info().equals(" "));
        }
        
        //Latitude & Longitude is not null
        for (int i = 0; i < 3; i ++) {
        	Assert.assertFalse(list.get(i).getLat().equals(" "));
        	Assert.assertFalse(list.get(i).getLon().equals(" "));
        	
        }

    }


}
