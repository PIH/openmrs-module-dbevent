package org.openmrs.module.dbevent;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Represents metadata for a Database
 */
@Data
@AllArgsConstructor
public class DatabaseJoin implements Serializable {

    private DatabaseColumn foreignKey;
    private DatabaseColumn primaryKey;

    /**
     * @return true if the foreign key column is nullable
     */
    public boolean isNullable() {
        return foreignKey.isNullable();
    }

    @Override
    public String toString() {
        return foreignKey + " -> " + primaryKey;
    }
}