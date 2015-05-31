package com.wkodate.stormtwitter.spout;

import backtype.storm.tuple.Values;
import com.wkodate.stormtwitter.Secret;
import com.wkodate.stormtwitter.Tweet;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.ITridentSpout;
import storm.trident.topology.TransactionAttempt;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * TweetEmitter.
 * <p>
 * Created by wkodate on 2015/05/24.
 */
public class TweetEmitter implements ITridentSpout.Emitter<Long> {

    private static final String TWEET_LANGUAGE = "ja";

    private TwitterStream stream;

    private LinkedBlockingQueue<Tweet> queue;

    public TweetEmitter() {
        queue = new LinkedBlockingQueue<>();
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
        stream.addListener(new TweetListener(this));
        stream.sample(TWEET_LANGUAGE);
    }

    public void emitBatch(
            TransactionAttempt tx, Long l, TridentCollector collector) {
        Tweet tweet = queue.poll();
        if (tweet == null) {
            return;
        }
        collector.emit(new Values(tweet.screenName, tweet.text, tweet.createdAt));
    }

    public void success(TransactionAttempt transactionAttempt) {
    }

    public void close() {
        System.out.println("TweetEmitter.close()");
    }

    public void offer(Tweet tw) {
        queue.offer(tw);
    }

}
