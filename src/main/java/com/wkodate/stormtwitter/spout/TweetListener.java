package com.wkodate.stormtwitter.spout;

import com.wkodate.stormtwitter.Tweet;
import twitter4j.StatusListener;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StallWarning;

/**
 * TweetListener
 *
 * Created by wkodate on 2015/05/30.
 */
public class TweetListener implements StatusListener{

    @Override
    public void onStatus(Status status) {
        Tweet tw = new Tweet(
                status.getUser().getScreenName(),
                status.getText().replaceAll(System.getProperty("line.separator"), ""),
                status.getUser().getCreatedAt().toString()
        );
        // TODO: queueに追加
        //tweetList.add(tw);

    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("*Got track limitation notice:" + numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("*Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);

    }

    @Override
    public void onStallWarning(StallWarning warning) {
        System.out.println("*Got stall warning:" + warning);

    }

    @Override
    public void onException(Exception e) {
        e.printStackTrace();
    }
}
