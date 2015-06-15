package com.wkodate.stormtwitter;

import backtype.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.HashMap;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TextSplitterTest
 *
 * @author wkodate
 */
public class TextSplitterTest {


    private TextSplitter textSplitter;

    private TridentCollector collector;

    @Before
    public void prepare() {
        this.textSplitter = new TextSplitter();
        TridentOperationContext context = mock(TridentOperationContext.class);
        textSplitter.prepare(new HashMap<>(), context);
        collector = mock(TridentCollector.class);
    }

    @Test
    public void MATest() {
        TridentTuple tuple = makeSampleTuple("今日はいい天気です。");

        textSplitter.execute(tuple, collector);

        verify(collector, times(6)).emit(new Values(anyString()));
    }

    @Test
    public void emptyTest() {
        TridentTuple tuple = makeSampleTuple("");

        textSplitter.execute(tuple, collector);

        verify(collector, never()).emit(new Values(anyString()));
    }

    @Test
    public void nullTest() {
        TridentTuple tuple = makeSampleTuple(null);

        textSplitter.execute(tuple, collector);

        verify(collector, never()).emit(new Values(anyString()));
    }

    private TridentTuple makeSampleTuple(String text) {
        TridentTuple tuple = mock(TridentTuple.class);
        when(tuple.getStringByField("text")).thenReturn(text);
        return tuple;
    }

}
