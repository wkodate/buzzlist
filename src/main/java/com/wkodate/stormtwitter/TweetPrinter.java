package com.wkodate.stormtwitter;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import twitter4j.*;

/**
 * TweetPrinter.
 *
 * Created by wkodate on 2015/05/14.
 */
public class TweetPrinter extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        String name = tuple.getStringByField("screen_name");
        String text = tuple.getStringByField("text");
        String createdAt = tuple.getStringByField("created_at");
        System.out.println(createdAt + "\t" + name + "\t" + text);
        collector.emit(new Values(name));
    }
}
