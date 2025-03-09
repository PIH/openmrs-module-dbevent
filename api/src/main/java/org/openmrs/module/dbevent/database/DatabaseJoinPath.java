package org.openmrs.module.dbevent.database;

import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents metadata for a Database
 */
@EqualsAndHashCode(callSuper = true)
public class DatabaseJoinPath extends ArrayList<DatabaseJoin> implements Serializable {

    /**
     * @return a new DatabaseJoinPath instance with each member added
     */
    public DatabaseJoinPath clone() {
        DatabaseJoinPath clone = new DatabaseJoinPath();
        clone.addAll(this);
        return clone;
    }

    /**
     * @return true if any of the joins in the path are nullable
     */
    public boolean isNullable() {
        boolean nullable = false;
        for (DatabaseJoin join : this) {
            nullable = nullable || join.isNullable();
        }
        return nullable;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}