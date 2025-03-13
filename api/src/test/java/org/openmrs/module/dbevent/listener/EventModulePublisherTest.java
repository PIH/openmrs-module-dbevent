package org.openmrs.module.dbevent.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.event.Event;
import org.openmrs.module.dbevent.DbEventContext;
import org.openmrs.module.dbevent.DbEventListenerConfig;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestUtils;

import javax.jms.MapMessage;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

@ExtendWith(MysqlExtension.class)
public class EventModulePublisherTest {

    private static final Logger log = LogManager.getLogger(EventModulePublisherTest.class);

    public static final String SOURCE = "TEST_SOURCE";

    @Test
    public void shouldPublishEvents() {
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, ctx);
        config.setDebeziumProperty("snapshot.mode", "when_needed");
        config.configureTablesToInclude(Arrays.asList("location"));

        final List<String> targetEvents = new ArrayList<>();

        Event.subscribe(EventModulePublisher.TOPIC_PREFIX + "location", message -> {
            try {
                MapMessage mapMessage = (MapMessage) message;
                Long timestamp = mapMessage.getLong("timestamp");
                String table = mapMessage.getString("table");
                String operation = mapMessage.getString("operation");
                Object key = mapMessage.getObject("key");
                assertThat(timestamp, notNullValue());
                assertThat(table, equalTo("location"));
                assertThat(operation, equalTo("READ"));
                assertThat(key, notNullValue());
                String event = timestamp + ":" + table + ":" + operation + ":" + key;
                targetEvents.add(event);
            }
            catch (Exception e) {
                log.error(e);
            }
        });

        EventModulePublisher listener = new EventModulePublisher();
        try {
            listener.init(config);
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 5);
        }
        finally {
            listener.stop();
        }

        long waitUntil = System.currentTimeMillis() + 10000;
        while (targetEvents.size() < 5 && System.currentTimeMillis() < waitUntil) {
            log.info("Waiting for all events to be received ({})", targetEvents.size());
        }

        assertThat(targetEvents.size(), greaterThanOrEqualTo(5));
        assertThat(targetEvents.size(), equalTo(Long.valueOf(listener.getStatus().getNumberOfReads()).intValue()));
    }
}
