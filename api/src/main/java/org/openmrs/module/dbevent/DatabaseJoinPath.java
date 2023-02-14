package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents metadata for a Database
 */
@Data
public class DatabaseJoinPath extends ArrayList<DatabaseJoin> implements Serializable {

    /**
     * @return a new DatabaseJoinPath instance with each member added
     */
    public DatabaseJoinPath clone() {
        DatabaseJoinPath clone = new DatabaseJoinPath();
        for (int i=0; i<size(); i++) {
            clone.add(get(i));
        }
        return clone;
    }

    /**
     * @return true if any of the joins in the path are nullable
     */
    public boolean isNullable() {
        boolean nullable = false;
        for (int i=0; i<size(); i++) {
            DatabaseJoin join = get(i);
            nullable = nullable || join.isNullable();
        }
        return nullable;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}