package com.wkodate.stormtwitter;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * TweetPrinter.
 *
 * Created by wkodate on 2015/05/14.
 */
public class TweetPrinter extends BaseFunction {

    public final void execute(
            final TridentTuple tuple, final TridentCollector collector) {
        String name = tuple.getStringByField("screen_name");
        String text = tuple.getStringByField("text");
        String createdAt = tuple.getStringByField("created_at");
        System.out.println(createdAt + "\t" + name + "\t" + text);
        collector.emit(new Values(name));
    }
}
