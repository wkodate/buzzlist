package com.wkodate.stormtwitter.spout;

import storm.trident.spout.ITridentSpout;

/**
 * TweetCoordinator.java
 * <p>
 * Created by wkodate on 2015/05/24.
 */
public class TweetCoordinator implements ITridentSpout.BatchCoordinator<Long> {

    public Long initializeTransaction(long txid, Long prevMetadata, Long currMetadata) {
        if (prevMetadata == null) {
            return 1L;
        }
        return prevMetadata + 1L;
    }

    public void success(long l) {

    }

    public boolean isReady(long l) {
        return true;
    }

    public void close() {
        System.out.println("TweetCoordinator.close()");
    }
}
