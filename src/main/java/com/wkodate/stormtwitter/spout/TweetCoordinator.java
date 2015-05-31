package com.wkodate.stormtwitter.spout;

import storm.trident.spout.ITridentSpout;

/**
 * TweetCoordinator.java
 * <p>
 * Created by wkodate on 2015/05/24.
 */
public class TweetCoordinator implements ITridentSpout.BatchCoordinator<Long> {

    public final Long initializeTransaction(
            final long txid, final Long prevMetadata, final Long currMetadata) {
        if (prevMetadata == null) {
            return 1L;
        }
        return prevMetadata + 1L;
    }

    public final void success(final long l) {

    }

    public final boolean isReady(final long l) {
        return true;
    }

    public final void close() {
        System.out.println("TweetCoordinator.close()");
    }
}
