package com.wkodate.stormtwitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import com.wkodate.stormtwitter.spout.TweetSpout;
import org.apache.commons.lang.StringUtils;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * NounCountTopology.
 *
 * @author wkodate
 */
public final class NounCountTopology {

    private NounCountTopology() {
    }

    public static StormTopology buildTopology(final LocalDRPC drpc) {
        TridentTopology topology = new TridentTopology();
        TweetSpout spout = new TweetSpout();
        TridentState state = topology.newStream("twitterSpout", spout)
                .parallelismHint(1)
                .shuffle()
                .each(
                        new Fields("text"),
                        new TextSplitter(),
                        new Fields("noun")
                )
                .parallelismHint(4)
                .groupBy(new Fields("noun"))
                .persistentAggregate(
                        new MemoryMapState.Factory(),
                        new Count(),
                        new Fields("count")
                )
                .parallelismHint(8);

        topology.newDRPCStream("words", drpc)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(
                        state,
                        new Fields("word"),
                        new MapGet(),
                        new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));
        return topology.build();
    }

    public static void main(final String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        if (args.length > 0 && StringUtils.isNotEmpty(args[0])) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, buildTopology(null));
        } else {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("TweetPrintTopology", conf, buildTopology(drpc));
            System.out.println("Topology submitted.");
            for (int i = 0; i < 100; i++) {
                System.out.println("count: " + drpc.execute("words", "http"));
                Thread.sleep(1000);
            }

        }
    }

}
