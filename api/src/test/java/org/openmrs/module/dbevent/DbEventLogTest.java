package org.openmrs.module.dbevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.test.EventMatcher;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventConsumer;
import org.openmrs.module.dbevent.test.TestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MysqlExtension.class)
public class DbEventLogTest {

    public static final String SOURCE = "TestSource";

    @Test
    public void shouldStreamAndMonitorEvents() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSourceConfig config = new DbEventSourceConfig(100002, SOURCE, ctx);
        config.configureTablesToInclude(Arrays.asList("encounter_type", "location"));
        DbEventSource eventSource = new DbEventSource(config);
        TestEventConsumer consumer = new TestEventConsumer();
        eventSource.setEventConsumer(consumer);
        try {
            eventSource.start();
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
            DbEventStatus status = DbEventLog.getLatestEventStatus(SOURCE);
            assertThat(status.getEvent(), equalTo(consumer.getLastEvent()));
            assertThat(status.getError(), nullValue());
            assertTrue(status.isProcessed());
            Map<String, Object> snapshotAttributes = DbEventLog.getSnapshotMonitoringAttributes(SOURCE);
            assertFalse(snapshotAttributes.isEmpty());
            Object value = snapshotAttributes.get("TotalTableCount");
            assertNotNull(value);
            int totalTableCount = Integer.parseInt(value.toString());
            assertThat(2, equalTo(totalTableCount));
        }
        finally {
            eventSource.stop();
        }
    }

    @Test
    public void shouldLogErrorOfLatestEvent() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSourceConfig config = new DbEventSourceConfig(100002, SOURCE, ctx);
        config.configureTablesToInclude(Collections.singletonList("encounter_type"));
        DbEventSource eventSource = new DbEventSource(config);
        int numMonitoredTables = eventSource.getConfig().getMonitoredTables().size();
        TestEventConsumer consumer = new TestEventConsumer();
        consumer.setSimulateErrorOnEvent(new EventMatcher(Operation.READ, "encounter_type", "encounter_type_id", 10L));
        eventSource.setEventConsumer(consumer);
        try {
            eventSource.start();
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 10);
            DbEventStatus status = DbEventLog.getLatestEventStatus(SOURCE);
            assertThat(status.getEvent(), equalTo(consumer.getLastEvent()));
            assertThat(status.getError(), nullValue());
            Map<String, Object> snapshotAttributes = DbEventLog.getSnapshotMonitoringAttributes(SOURCE);
            assertFalse(snapshotAttributes.isEmpty());
            Object value = snapshotAttributes.get("TotalTableCount");
            assertNotNull(value);
            int totalTableCount = Integer.parseInt(value.toString());
            assertThat(numMonitoredTables, equalTo(totalTableCount));
        }
        finally {
            eventSource.stop();
        }
    }
}
