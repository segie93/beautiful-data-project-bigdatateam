package edu.csula.datascience.acquisition;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import java.util.List;

public class CollectorTest {
    private Collector<SimpleModel, MockData> collector;
    private Source<MockData> source;

    @Before
    public void setup() {
        collector = new MockCollector();
        source = new MockSource();
    }

    @Test
    @Ignore
    public void mungee() throws Exception {
        List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
       
        
        
       /* List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("2", "content2"),
            new SimpleModel("3", "content3")
        );*/

        Assert.assertEquals(list.size(), 3);

        for (int i = 0; i < 2; i ++) {
            Assert.assertEquals(list.get(i).getCountryCode(), "US");
            Assert.assertEquals(list.get(i).getCountryCode(), "US");
        }
    }
    
    
   /* @Test
    public void mungee() throws Exception {
        List<SimpleModel> list = (List<SimpleModel>) collector.mungee(source.next());
        List<SimpleModel> expectedList = Lists.newArrayList(
            new SimpleModel("2", "content2"),
            new SimpleModel("3", "content3")
        );

        Assert.assertEquals(list.size(), 2);

        for (int i = 0; i < 2; i ++) {
            Assert.assertEquals(list.get(i).getId(), expectedList.get(i).getId());
            Assert.assertEquals(list.get(i).getContent(), expectedList.get(i).getContent());
        }
    }*/
}