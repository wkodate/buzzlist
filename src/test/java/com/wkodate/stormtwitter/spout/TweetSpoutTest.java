package com.wkodate.stormtwitter.spout;

import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import org.junit.Before;
import org.junit.Test;
import storm.trident.spout.ITridentSpout;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;


/**
 * TweetSpoutTest.
 *
 * @author wkodate;
 *
 */
public class TweetSpoutTest {

    private TweetSpout spout;

    private TopologyContext context;

    @Before
    public void prepare() {
        this.spout = new TweetSpout();
        context = mock(TopologyContext.class);
    }

    /**
     * getCoordinatorのテスト.
     */
    @Test
    public void getCoordinatorTest() {
        ITridentSpout.BatchCoordinator coordinator
                = spout.getCoordinator("", new HashMap<>(), context);
        assertThat(coordinator, instanceOf(TweetCoordinator.class));
    }

    /**
     * getEmitterのテスト.
     */
    @Test
    public void getEmitterTest() {
        ITridentSpout.Emitter emitter
                = spout.getEmitter("", new HashMap<>(), context);
        assertThat(emitter, instanceOf(ITridentSpout.Emitter.class));

    }

    /**
     * getComponentConfigurationのテスト.
     */
    @Test
    public void getComponentConfigurationTest() {
        Map config = spout.getComponentConfiguration();
        assertThat(config, instanceOf(Config.class));

    }

    /**
     * getOutputFieldsのテスト.
     */
    @Test
    public void getOutputFieldsTest() {
        Fields fields = spout.getOutputFields();
        assertThat(fields, instanceOf(Fields.class));

    }
}
