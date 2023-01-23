package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents metadata for a Database
 */
@Data
public class DatabaseMetadata implements Serializable {
    private String databaseName;
    private Map<String, DatabaseTable> tables = new LinkedHashMap<>();

    public void addTable(DatabaseTable table) {
        tables.put(table.getTableName(), table);
    }
}