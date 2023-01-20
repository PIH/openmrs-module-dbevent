package org.openmrs.module.dbevent;

import io.debezium.engine.ChangeEvent;
import lombok.Data;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Serializable;

/**
 * Represents a Database Change Event
 */
@Data
public class DbEvent implements Serializable {

    private final ChangeEvent<SourceRecord, SourceRecord> changeEvent;
    private final Long timestamp;
    private final String sourceName;
    private final String table;
    private final Operation operation;
    private final ObjectMap key;
    private final ObjectMap before;
    private final ObjectMap after;
    private final ObjectMap values;
    private final ObjectMap source;

    /**
     * Constructor that creates a DbEvent from a Debezium ChangeEvent.
     * Note, that if tombstones are enabled, this will need to become null-safe on record.getValue()
     * @param changeEvent the Debezium ChangeEvent
     */
    public DbEvent(ChangeEvent<SourceRecord, SourceRecord> changeEvent) {
        this.changeEvent = changeEvent;
        try {
            SourceRecord record = changeEvent.value();
            key = new ObjectMap((Struct) record.key());
            Struct valueStruct = (Struct) record.value();
            timestamp = valueStruct.getInt64("ts_ms");
            operation = Operation.parse(valueStruct.getString("op"));
            before = new ObjectMap(valueStruct.getStruct("before"));
            after = new ObjectMap(valueStruct.getStruct("after"));
            values = (operation == Operation.DELETE ? before : after);
            source = new ObjectMap(valueStruct.getStruct("source"));
            table = source.getString("table");
            sourceName = source.getString("name");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getUuid() {
        return getValues().getString("uuid");
    }

    @Override
    public String toString() {
        return operation + " " + table + " " + key;
    }
}