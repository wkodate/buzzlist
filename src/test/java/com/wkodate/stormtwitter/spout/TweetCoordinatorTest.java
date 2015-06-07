package com.wkodate.stormtwitter.spout;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


/**
 * TweetCoordinatorTest.
 *
 * @author wkodate;
 */
public class TweetCoordinatorTest {

    private TweetCoordinator coordinator;

    @Before
    public void prepare() {
        this.coordinator = new TweetCoordinator();
    }

    /**
     * initializeTransactionのテスト.
     */
    @Test
    public void initializeTransactionTest() {
        Long meta = coordinator.initializeTransaction(0L, 1L, 2L);
        assertThat(meta, is(2L));
    }

    /**
     * 前のmetadataがnullのときのテスト.
     */
    @Test
    public void firstMetaTest() {
        Long meta = coordinator.initializeTransaction(0L, null, null);
        assertThat(meta, is(1L));
    }

}
