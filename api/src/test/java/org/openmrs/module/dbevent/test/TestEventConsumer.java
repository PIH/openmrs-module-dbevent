package org.openmrs.module.dbevent.test;

import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.EventConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple test event consumer
 */
public class TestEventConsumer implements EventConsumer {

    private final List<DbEvent> events = new ArrayList<>();
    private Integer forceErrorAfterNum = null;

    @Override
    public void accept(DbEvent event) {
        if (forceErrorAfterNum != null && events.size() == forceErrorAfterNum) {
            throw new RuntimeException("TEST_ERROR");
        }
        events.add(event);
        if (forceErrorAfterNum != null && events.size() == forceErrorAfterNum) {
            throw new RuntimeException("TEST_ERROR");
        }
    }

    public List<DbEvent> getEvents() {
        return events;
    }

    public DbEvent getLastEvent() {
        int size = getEvents().size();
        return size == 0 ? null : getEvents().get(size-1);
    }

    public void setForceErrorAfterNum(int forceErrorAfterNum) {
        this.forceErrorAfterNum = forceErrorAfterNum;
    }
}
