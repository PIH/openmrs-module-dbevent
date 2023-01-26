package org.openmrs.module.dbevent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.test.EventMatcher;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventConsumer;
import org.openmrs.module.dbevent.test.TestUtils;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MysqlExtension.class)
public class DbEventSourceTest {

    private static final Logger log = LogManager.getLogger(DbEventSourceTest.class);

    public static final String SOURCE = "TEST_SOURCE";

    @Test
    public void shouldStartAndStopEventSource() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSourceConfig config = new DbEventSourceConfig(100002, SOURCE, ctx);
        config.configureTablesToInclude(Arrays.asList("location", "encounter_type"));
        config.setRetryIntervalSeconds(1);
        DbEventSource eventSource = new DbEventSource(config);
        TestEventConsumer eventConsumer = new TestEventConsumer();
        eventSource.setEventConsumer(eventConsumer);
        try {
            eventSource.start();
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
        }
        finally {
            eventSource.stop();
            log.debug("Event source stopped.  Num events received: " + eventConsumer.getNumEvents());
        }
        assertTrue(eventConsumer.getNumEvents() >= 10);
    }

    @Test
    public void shouldStartAndStopAndRestart() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSourceConfig config = new DbEventSourceConfig(100002, SOURCE, ctx);
        config.configureTablesToInclude(Collections.singletonList("location"));
        DbEventSource eventSource = new DbEventSource(config);
        TestEventConsumer eventConsumer = new TestEventConsumer();
        eventConsumer.setSimulateErrorOnEvent(new EventMatcher(Operation.UPDATE, "location", "location_id", 2));
        eventSource.setEventConsumer(eventConsumer);
        final Database db = ctx.getDatabase();
        try {
            eventSource.start();
            TestUtils.waitForSnapshotToStart(SOURCE);
            db.executeUpdate("update location set date_changed = now() where location_id = 1");
            db.executeUpdate("update location set date_changed = now() where location_id = 2");
            TestUtils.waitForNumberOfStreamingEvents(SOURCE, 1);
        }
        finally {
            eventSource.stop();
        }
        EventMatcher matcher = new EventMatcher(Operation.UPDATE, "location", "location_id", 1);
        assertTrue(matcher.matches(eventConsumer.getLastEvent()));

        // Remove the forced error condition and restart
        eventConsumer.getEvents().clear();
        eventConsumer.setSimulateErrorOnEvent(null);
        try {
            eventSource.start();
            TestUtils.waitForNumberOfStreamingEvents(SOURCE, 1);
        }
        finally {
            eventSource.stop();
        }
        assertThat(eventConsumer.getNumEvents(), equalTo(1));
        matcher = new EventMatcher(Operation.UPDATE, "location", "location_id", 2);
        assertTrue(matcher.matches(eventConsumer.getLastEvent()));
    }
}
