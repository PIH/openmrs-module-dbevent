package org.openmrs.module.dbevent;

import lombok.Data;
import lombok.EqualsAndHashCode;

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
    private boolean nullable;

    @EqualsAndHashCode.Exclude private Set<DatabaseColumn> referencedBy = new HashSet<>();
    @EqualsAndHashCode.Exclude private Set<DatabaseColumn> references = new HashSet<>();

    public DatabaseColumn(String databaseName, String tableName, String columnName, boolean nullable) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.nullable = nullable;
    }

    /**
     * @return the fully qualified column name as tableName.columnName
     */
    public String getTableAndColumn() {
        return tableName + "." + columnName;
    }

    @Override
    public String toString() {
        return getTableAndColumn();
    }
}