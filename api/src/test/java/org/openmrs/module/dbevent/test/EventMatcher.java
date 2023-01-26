package org.openmrs.module.dbevent.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.Operation;

import java.util.Objects;

/**
 * Simple class to match on a particular DbEvent
 */
@Data
@AllArgsConstructor
public class EventMatcher {

    private Operation operation;
    private String tableName;
    private String columnName;
    private Object columnValue;

    /**
     * @param dbEvent the DbEvent to match
     * @return true if the specified values match the given DbEvent
     */
    public boolean matches(DbEvent dbEvent) {
        if (operation != null && !operation.equals(dbEvent.getOperation())) {
            return false;
        }
        if (tableName != null && !tableName.equals(dbEvent.getTable())) {
            return false;
        }
        if (columnName != null) {
            Object val = dbEvent.getValues().get(columnName);
            return Objects.equals(columnValue, val);
        }
        return true;
    }
}
