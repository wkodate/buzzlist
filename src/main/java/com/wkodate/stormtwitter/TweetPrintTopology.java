package com.wkodate.stormtwitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.wkodate.stormtwitter.spout.TweetSpout;
import org.apache.commons.lang.StringUtils;
import storm.trident.TridentTopology;

/**
 * TweetPrintTopology.
 *
 * @author wkodate
 */
public final class TweetPrintTopology {

    private TweetPrintTopology() {

    }

    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        TweetSpout spout = new TweetSpout();
        topology.newStream("twitterSpout", spout)
                .parallelismHint(1)
                .shuffle()
                .each(
                        new Fields("screen_name", "text", "created_at"),
                        new TweetPrinter(),
                        new Fields("dev_null")
                )
                .parallelismHint(4);
        return topology.build();
    }

    public static void main(final String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        conf.setNumWorkers(3);
        if (args.length > 0 && StringUtils.isNotEmpty(args[0])) {
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("TweetPrintTopology", conf, buildTopology());
        }
    }

}
