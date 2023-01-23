package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;

/**
 * Represents metadata for a Database
 */
@Data
public class DatabaseColumn implements Serializable {
    private String databaseName;
    private String tableName;
    private String columnName;

    public DatabaseColumn(String databaseName, String tableName, String columnName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
    }
}