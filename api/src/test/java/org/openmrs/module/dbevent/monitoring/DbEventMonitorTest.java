package org.openmrs.module.dbevent.monitoring;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.DbEventContext;
import org.openmrs.module.dbevent.DbEventListenerConfig;
import org.openmrs.module.dbevent.Operation;
import org.openmrs.module.dbevent.test.EventMatcher;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventListener;
import org.openmrs.module.dbevent.test.TestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MysqlExtension.class)
public class DbEventMonitorTest {

    public static final String SOURCE = "TestSource";

    @Test
    public void shouldStreamAndMonitorEvents() {
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, new Properties(), ctx);
        config.setProperty("debezium.snapshot.mode", "when_needed");
        config.configureTablesToInclude(Arrays.asList("encounter_type", "location"));
        TestEventListener listener = new TestEventListener();
        try {
            listener.init(config);
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
            DbEventListenerStatus s = DbEventMonitor.getDbEventListenerStatus(listener);
            assertNotNull(s);
            assertThat(s.getStatus(), equalTo("OK"));
            assertThat(s.getLatestEvent(), equalTo(listener.getLastEvent().toString()));
            assertThat(s.isLatestEventProcessed(), equalTo(true));
            assertThat(s.getLatestEventErrorMessage(), nullValue());
            assertThat(s.getLatestEventErrorRetryNum(), nullValue());

            Map<String, Object> snapshotAttributes = DbEventMonitor.getSnapshotMonitoringAttributes(SOURCE);
            assertFalse(snapshotAttributes.isEmpty());
            Object value = snapshotAttributes.get("TotalTableCount");
            assertNotNull(value);
            int totalTableCount = Integer.parseInt(value.toString());
            assertThat(2, equalTo(totalTableCount));
        }
        finally {
            listener.stop();
        }
    }

    @Test
    public void shouldLogErrorOfLatestEvent() {
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, new Properties(), ctx);
        config.setProperty("debezium.snapshot.mode", "when_needed");
        config.configureTablesToInclude(Collections.singletonList("encounter_type"));
        TestEventListener listener = new TestEventListener();
        listener.setSimulateErrorOnEvent(new EventMatcher(Operation.READ, "encounter_type", "encounter_type_id", 10L));
        try {
            listener.init(config);
            int numMonitoredTables = listener.getConfig().getMonitoredTables().size();
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
            DbEventListenerStatus status = DbEventMonitor.getDbEventListenerStatus(listener);
            assertNotNull(status);
            assertThat(status.getLatestEvent(), equalTo(listener.getLastEvent().toString()));
            assertThat(status.getLatestEventErrorMessage(), nullValue());
            Map<String, Object> snapshotAttributes = DbEventMonitor.getSnapshotMonitoringAttributes(SOURCE);
            assertFalse(snapshotAttributes.isEmpty());
            Object value = snapshotAttributes.get("TotalTableCount");
            assertNotNull(value);
            int totalTableCount = Integer.parseInt(value.toString());
            assertThat(numMonitoredTables, equalTo(totalTableCount));
        }
        finally {
            listener.stop();
        }
    }
}
