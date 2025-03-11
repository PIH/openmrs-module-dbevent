package org.openmrs.module.dbevent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.database.Database;
import org.openmrs.module.dbevent.test.EventMatcher;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventListener;
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
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, ctx);
        config.setProperty("debezium.snapshot.mode", "when_needed");
        config.configureTablesToInclude(Arrays.asList("location", "encounter_type"));
        config.getListenerConfig().setProperty("retryIntervalMillis", "1000");
        TestEventListener listener = new TestEventListener();
        try {
            listener.init(config);
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
        }
        finally {
            listener.stop();
            log.debug("Event source stopped.  Num events received: " + listener.getNumEvents());
        }
        assertTrue(listener.getNumEvents() >= 10);
    }

    @Test
    public void shouldStartAndStopAndRestart() throws Exception {
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, ctx);
        config.setProperty("debezium.snapshot.mode", "when_needed");
        config.configureTablesToInclude(Collections.singletonList("location"));
        TestEventListener listener = new TestEventListener();
        listener.setSimulateErrorOnEvent(new EventMatcher(Operation.UPDATE, "location", "location_id", 2));
        final Database db = ctx.getDatabase();
        try {
            listener.init(config);
            TestUtils.waitForSnapshotToStart(SOURCE);
            db.executeUpdate("update location set date_changed = now() where location_id = 1");
            db.executeUpdate("update location set date_changed = now() where location_id = 2");
            TestUtils.waitForNumberOfStreamingEvents(SOURCE, 1);
        }
        finally {
            listener.stop();
        }
        EventMatcher matcher = new EventMatcher(Operation.UPDATE, "location", "location_id", 1);
        assertTrue(matcher.matches(listener.getLastEvent()));

        // Remove the forced error condition and restart
        listener.getEvents().clear();
        listener.setSimulateErrorOnEvent(null);
        try {
            listener.start();
            TestUtils.waitForNumberOfStreamingEvents(SOURCE, 1);
        }
        finally {
            listener.stop();
        }
        assertThat(listener.getNumEvents(), equalTo(1));
        matcher = new EventMatcher(Operation.UPDATE, "location", "location_id", 2);
        assertTrue(matcher.matches(listener.getLastEvent()));
    }
}
