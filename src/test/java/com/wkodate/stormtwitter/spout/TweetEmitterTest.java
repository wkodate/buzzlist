package com.wkodate.stormtwitter.spout;

import backtype.storm.tuple.Values;
import com.wkodate.stormtwitter.Tweet;
import org.junit.Before;
import org.junit.Test;
import storm.trident.operation.TridentCollector;
import storm.trident.topology.TransactionAttempt;

import java.lang.reflect.Field;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;


/**
 * TweetEmitterTest.
 *
 * @author wkodate;
 */
public class TweetEmitterTest {

    private static final String SAMPLE_TWEET_SCREEN_NAME = "name";

    private static final String SAMPLE_TWEET_TEXT = "helloworld";

    private static final String SAMPLE_TWEET_CREATED_AT = "2015-06-07";

    private TweetEmitter emitter;

    private Field queueField;

    @Before
    public void prepare() {
        this.emitter = new TweetEmitter();
        Class c = emitter.getClass();
        try {
            queueField = c.getDeclaredField("queue");
        } catch (NoSuchFieldException e) {
            fail();
            e.printStackTrace();
        }
        queueField.setAccessible(true);
    }

    /**
     * offerのテスト.
     */
    @Test
    public void offerTest() {
        final int produceCount = 3;
        for (int i = 0; i < produceCount; i++) {
            emitter.offer(createSampleTweetInstance());
        }

        LinkedBlockingQueue queue = getQueue();
        assertThat(queue.size(), is(produceCount));
    }

    /**
     * emitBatchのテスト.
     */
    @Test
    public void emitBatchTest() {
        emitter.offer(createSampleTweetInstance());

        TridentCollector collector = mock(TridentCollector.class);
        TransactionAttempt txAttempt = mock(TransactionAttempt.class);
        emitter.emitBatch(txAttempt, 0L, collector);

        verify(collector).emit(new Values(
                SAMPLE_TWEET_SCREEN_NAME, SAMPLE_TWEET_TEXT, SAMPLE_TWEET_CREATED_AT));
    }

    /**
     * queueにデータがないときはemitしない.
     */
    @Test
    public void noEmitTest() {
        TridentCollector collector = mock(TridentCollector.class);
        TransactionAttempt txAttempt = mock(TransactionAttempt.class);
        emitter.emitBatch(txAttempt, 0L, collector);

        verify(collector, never()).emit(new Values(
                SAMPLE_TWEET_SCREEN_NAME, SAMPLE_TWEET_TEXT, SAMPLE_TWEET_CREATED_AT));
    }

    /**
     * Tweetのインスタンスを生成して返す.
     *
     * @return Tweet
     */
    private Tweet createSampleTweetInstance() {
        return new Tweet(SAMPLE_TWEET_SCREEN_NAME, SAMPLE_TWEET_TEXT, SAMPLE_TWEET_CREATED_AT);
    }

    /**
     * Queueを取得.
     *
     * @return queue
     */
    private LinkedBlockingQueue getQueue() {
        try {
            return (LinkedBlockingQueue) queueField.get(emitter);
        } catch (IllegalAccessException e) {
            fail();
            e.printStackTrace();
        }
        return new LinkedBlockingQueue();
    }

}
