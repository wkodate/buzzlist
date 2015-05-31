package com.wkodate.stormtwitter.spout;

import com.wkodate.stormtwitter.Tweet;
import twitter4j.StatusListener;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StallWarning;

/**
 * TweetListener
 * <p>
 * Created by wkodate on 2015/05/30.
 */
public class TweetListener implements StatusListener {

    private final TweetEmitter tweetEmitter;

    public TweetListener(final TweetEmitter emitter) {
        tweetEmitter = emitter;
    }

    @Override
    public final void onStatus(final Status status) {
        Tweet tw = new Tweet(
                status.getUser().getScreenName(),
                status.getText().replaceAll(System.getProperty("line.separator"), ""),
                status.getUser().getCreatedAt().toString()
        );
        tweetEmitter.offer(tw);
    }

    @Override
    public final void onDeletionNotice(final StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public final void onTrackLimitationNotice(final int numberOfLimitedStatuses) {
        System.out.println("*Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public final void onScrubGeo(final long userId, final long upToStatusId) {
        System.out.println("*Got scrub_geo event userId:" + userId
                + " upToStatusId:" + upToStatusId);

    }

    @Override
    public final void onStallWarning(final StallWarning warning) {
        System.out.println("*Got stall warning:" + warning);

    }

    @Override
    public final void onException(final Exception e) {
        e.printStackTrace();
    }
}
