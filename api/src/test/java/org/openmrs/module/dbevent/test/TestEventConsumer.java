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

    @Override
    public void accept(DbEvent event) {
        events.add(event);
    }

    public List<DbEvent> getEvents() {
        return events;
    }
}
