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

    public BatchCoordinator getCoordinator(String s, Map map, TopologyContext topologyContext) {
        System.out.println("TweetSpout.getCoordinator(): s=" + s + ", map=" + map
                + ", context=" + topologyContext);
        return new TweetCoordinator();
    }

    public Emitter getEmitter(String s, Map map, TopologyContext topologyContext) {
        System.out.println("TweetSpout.getEmitter(): s=" + s + ", map=" + map
                + ", context=" + topologyContext);
        return new TweetEmitter();
    }

    public Map getComponentConfiguration() {
        System.out.println("TweetSpout.getComponentConfiguration()");
        return new Config();
    }

    public Fields getOutputFields() {
        System.out.println("TweetSpout.getOutputFields()");
        return new Fields("screen_name", "text", "created_at");
    }

}
