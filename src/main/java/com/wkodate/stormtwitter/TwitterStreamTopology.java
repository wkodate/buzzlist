package com.wkodate.stormtwitter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.wkodate.stormtwitter.spout.TwitterStreamSpout;
import storm.trident.TridentTopology;

/**
 * TwitterStreamTopology
 */
public class TwitterStreamTopology {

    public static StormTopology buildTopology() {
        TwitterStreamSpout spout = new TwitterStreamSpout();
        TridentTopology topology = new TridentTopology();
        topology.newStream("twitterSpout", spout)
                .each(
                        new Fields("screen_name", "text", "created_at"),
                        new PrintSampleStream(),
                        new Fields("dev_null")
                )
                .parallelismHint(4);
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology());
    }

}
