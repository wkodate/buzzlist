package com.wkodate.stormtwitter;

import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import static org.mockito.Mockito.*;

/**
 * TweetPrinterTest
 *
 * @author wkodate
 */
public class TweetPrinterTest {


    private TweetPrinter tweetPrinter;
    private TridentCollector collector;

    @Before
    public void prepare() {
        this.tweetPrinter = new TweetPrinter();
        collector = mock(TridentCollector.class);
    }

    @Test
    public void executeTest() {
        TridentTuple tuple = mock(TridentTuple.class);
        String name = "ichiro";
        String text = "i like baseball";
        String date = "2015-06-08";
        when(tuple.getStringByField("screen_name")).thenReturn(name);
        when(tuple.getStringByField("text")).thenReturn(text);
        when(tuple.getStringByField("created_at")).thenReturn(date);

        tweetPrinter.execute(tuple, collector);

        verify(collector).emit(new Values(name));
    }

}
