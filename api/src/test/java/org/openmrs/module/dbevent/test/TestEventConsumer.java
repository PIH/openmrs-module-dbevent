package org.openmrs.module.dbevent.test;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.EventConsumer;
import org.openmrs.module.dbevent.Operation;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * Simple test event consumer
 */
@Data
public class TestEventConsumer implements EventConsumer {

    private static final Logger log = LogManager.getLogger(TestEventConsumer.class);

    private final List<DbEvent> events = new CopyOnWriteArrayList<>();
    private EventMatcher simulateErrorOnEvent = null;

    @Override
    public void accept(DbEvent event) {
        if (simulateErrorOnEvent != null && simulateErrorOnEvent.matches(event)) {
            throw new RuntimeException("TEST_ERROR");
        }
        events.add(event);
    }

    public void waitForEvents(String tableName, Operation operation, int num) {
        while (getEvents(tableName, operation).size() < num) {
            log.trace("Waiting for " + num + " events");
        }
    }

    public int getNumEvents() {
        return events.size();
    }

    public DbEvent getLastEvent() {
        int size = getEvents().size();
        return size == 0 ? null : getEvents().get(size-1);
    }

    @Override
    public String toString() {
        return "TestEventConsumer with " + events.size() + " events";
    }

    public List<DbEvent> getEvents(String tableName, Operation operation) {
        return events.stream()
                .filter(e -> e.getTable().equals(tableName) && e.getOperation() == operation)
                .collect(Collectors.toList());
    }
}
