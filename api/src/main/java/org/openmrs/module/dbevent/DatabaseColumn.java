package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Represents metadata for a Database
 */
@Data
public class DatabaseColumn implements Serializable {
    private String databaseName;
    private String tableName;
    private String columnName;
    private boolean primaryKey;
    private Set<DatabaseColumn> externalReferences = new HashSet<>();

    public DatabaseColumn(String databaseName, String tableName, String columnName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        return columnName;
    }
}