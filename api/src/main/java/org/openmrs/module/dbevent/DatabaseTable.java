package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Represents metadata for a Database
 */
@Data
public class DatabaseTable implements Serializable {
    private String databaseName;
    private String tableName;
    private Map<String, DatabaseColumn> columns = new LinkedHashMap<>();

    public DatabaseTable(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public void addColumn(DatabaseColumn column) {
        columns.put(column.getColumnName(), column);
    }

    public Set<String> getTablesReferencedBy() {
        Set<String> ret = new HashSet<>();
        for (DatabaseColumn column : columns.values()) {
            for (DatabaseColumn dependentColumn : column.getReferencedBy()) {
                ret.add(dependentColumn.getTableName());
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        return tableName;
    }
}