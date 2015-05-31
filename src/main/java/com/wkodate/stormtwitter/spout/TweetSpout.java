package com.wkodate.stormtwitter.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.spout.ITridentSpout;

import java.util.Map;

/**
 * TweetSpout.java
 * <p>
 * Created by wkodate on 2015/04/24.
 */
public class TweetSpout implements ITridentSpout {

    public final BatchCoordinator getCoordinator(
            final String str, final Map map, final TopologyContext context) {
        System.out.println("TweetSpout.getCoordinator(): s=" + str + ", map=" + map
                + ", context=" + context);
        return new TweetCoordinator();
    }

    public final Emitter getEmitter(
            final String str, final Map map, final TopologyContext topologyContext) {
        System.out.println("TweetSpout.getEmitter(): s=" + str + ", map=" + map
                + ", context=" + topologyContext);
        return new TweetEmitter();
    }

    public final Map getComponentConfiguration() {
        System.out.println("TweetSpout.getComponentConfiguration()");
        return new Config();
    }

    public final Fields getOutputFields() {
        System.out.println("TweetSpout.getOutputFields()");
        return new Fields("screen_name", "text", "created_at");
    }

}
