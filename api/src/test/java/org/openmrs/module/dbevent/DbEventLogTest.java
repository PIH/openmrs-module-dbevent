package org.openmrs.module.dbevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventConsumer;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(MysqlExtension.class)
public class DbEventLogTest {

    public static final String SOURCE = "TestSource";

    @Test
    public void shouldStreamAndMonitorEvents() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSource eventSource = new DbEventSource(new DbEventSourceConfig(100002, SOURCE, ctx));
        int numMonitoredTables = eventSource.getConfig().getMonitoredTables().size();
        TestEventConsumer consumer = new TestEventConsumer();
        consumer.setForceErrorAfterNum(10);
        eventSource.setEventConsumer(consumer);
        try {
            eventSource.start();
            while (consumer.getEvents().size() < 10) {
                Thread.sleep(1000);
            }
            DbEventStatus status = DbEventLog.getLatestEventStatus(SOURCE);
            assertThat(status.getEvent(), equalTo(consumer.getLastEvent()));
            assertThat(status.getError(), notNullValue());
            assertFalse(status.isProcessed());
            List<String> attNames = DbEventLog.getMonitoringBeanAttributeNames(SOURCE);
            assertFalse(attNames.isEmpty());
            Object value = DbEventLog.getMonitoringBeanAttribute(SOURCE, "TotalTableCount");
            assertNotNull(value);
            int totalTableCount = Integer.parseInt(value.toString());
            assertThat(numMonitoredTables, equalTo(totalTableCount));
        }
        finally {
            eventSource.stop();
        }
    }
}
