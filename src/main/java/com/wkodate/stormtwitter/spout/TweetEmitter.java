package com.wkodate.stormtwitter.spout;

import backtype.storm.tuple.Values;
import com.wkodate.stormtwitter.Secret;
import com.wkodate.stormtwitter.Tweet;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;


/**
 * TweetEmitter.
 *
 * Created by wkodate on 2015/05/24.
 */
public class TweetEmitter implements ITridentSpout.Emitter<Long> {

    private int index;

    private TwitterStream stream;

    private final List<Tweet> tweetList;

    public TweetEmitter(int index) {
        this.index = index;
        tweetList = new CopyOnWriteArrayList<>();
        initTwitterStream();
    }

    private void initTwitterStream() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(Secret.CONSUMER_KEY)
                .setOAuthConsumerSecret(Secret.CONSUMER_SECRET)
                .setOAuthAccessToken(Secret.ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(Secret.ACCESS_TOKEN_SECRET);
        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        stream = factory.getInstance();
        try {
            initStreamListener();
            stream.sample("ja");
        } catch(TwitterException e) {
            e.getStackTrace();
        }
    }

    public void emitBatch(
            TransactionAttempt tx, Long l, TridentCollector collector) {
        if (tweetList.isEmpty()) {
            return;
        }
        Iterator iterator = tweetList.iterator();
            while (iterator.hasNext()) {
                Tweet tw = (Tweet) iterator.next();
                collector.emit(new Values(tw.screenName, tw.text, tw.createdAt));
            }
        tweetList.clear();
    }

    public void success(TransactionAttempt transactionAttempt) {
    }

    public void close() {
        System.out.println("TweetEmitter.close()");
    }

    private void initStreamListener() throws TwitterException{
        StatusListener listener = new StatusListener() {
            public void onStatus(Status status) {
                Tweet tw = new Tweet(
                        status.getUser().getScreenName(),
                        status.getText().replaceAll(System.getProperty("line.separator"), ""),
                        status.getUser().getCreatedAt().toString()
                );
                tweetList.add(tw);
            }

            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("*Got track limitation notice:" + numberOfLimitedStatuses);
            }

            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("*Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            public void onStallWarning(StallWarning warning) {
                System.out.println("*Got stall warning:" + warning);
            }

            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        stream.addListener(listener);
    }
}
