package org.openmrs.module.dbevent.listener;

import org.openmrs.event.Event;
import org.openmrs.event.EventMessage;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventListener;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

/**
 * DbEventListener which uses the Event module to publish each DbEvent within the Event module
 * for consumption by JMS Event Listeners
 */
public class EventModulePublisher extends DbEventListener {

    private final String topicName;

    public EventModulePublisher(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public void processEvent(DbEvent dbEvent) {
        EventMessage eventMessage = new EventMessage();
        eventMessage.put("timestamp", dbEvent.getTimestamp());
        eventMessage.put("table", dbEvent.getTable());
        eventMessage.put("operation", dbEvent.getOperation().name());
        eventMessage.put("key", dbEvent.getKey());
        eventMessage.put("source", dbEvent.getSource());

        ArrayList<String> columns = new ArrayList<>(dbEvent.getValues().keySet());
        eventMessage.put("columns", columns);

        HashMap<String, Serializable> beforeValues = new HashMap<>();
        HashMap<String, Serializable> afterValues = new HashMap<>();
        ArrayList<String> changedValues = new ArrayList<>();
        ArrayList<String> nonSerializableValues = new ArrayList<>();

        for (String column : columns) {
            Object beforeValue = dbEvent.getBefore().get(column);
            Object afterValue = dbEvent.getAfter().get(column);
            boolean valueChanged = !Objects.equals(beforeValue, afterValue);
            boolean beforeSerializable = beforeValue == null || beforeValue instanceof Serializable;
            boolean afterSerializable = afterValue == null || afterValue instanceof Serializable;
            if (beforeSerializable && afterSerializable) {
                beforeValues.put(column, (Serializable) beforeValue);
                afterValues.put(column, (Serializable) afterValue);
            }
            else {
                nonSerializableValues.add(column);
            }
            if (valueChanged) {
                changedValues.add(column);
            }
        }
        eventMessage.put("before", beforeValues);
        eventMessage.put("after", afterValues);
        eventMessage.put("changed", changedValues);
        eventMessage.put("nonSerializable", nonSerializableValues);

        Event.fireEvent(topicName, eventMessage);
    }
}
