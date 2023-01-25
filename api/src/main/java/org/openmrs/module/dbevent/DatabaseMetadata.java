package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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

    public Set<String> getAllTablesReferencedBy(String tableName, String... excludedTables) {
        Set<String> ret = new HashSet<>(tables.get(tableName).getTablesReferencedBy());
        Arrays.asList(excludedTables).forEach(ret::remove);
        List<String> nestedTables = new ArrayList<>(ret);
        for (String nestedTable : nestedTables) {
            List<String> nestedExclusions = new ArrayList<>(Arrays.asList(excludedTables));
            nestedExclusions.addAll(ret);
            ret.addAll(getAllTablesReferencedBy(nestedTable, nestedExclusions.toArray(new String[0])));
        }
        return ret;
    }

    public Set<String> getPatientTableNames() {
        Set<String> ret = new TreeSet<>();
        ret.addAll(getAllTablesReferencedBy("patient"));
        ret.addAll(getAllTablesReferencedBy("person", "users", "provider"));
        return ret;
    }

    public void print() {
        for (DatabaseTable table : getTables().values()) {
            System.out.println("=======================");
            System.out.println("TABLE: " + table.getTableName());
            for (DatabaseColumn column : table.getColumns().values()) {
                System.out.println(" " + column.getColumnName() + (column.isPrimaryKey() ? " PRIMARY KEY" : ""));
                for (DatabaseColumn fkColumn : column.getReferences()) {
                    System.out.println("   => " + fkColumn.getTableName() + "." + fkColumn.getColumnName());
                }
                for (DatabaseColumn fkColumn : column.getReferencedBy()) {
                    System.out.println("   <= " + fkColumn.getTableName() + "." + fkColumn.getColumnName());
                }
            }
        }
    }

    public DatabaseTable getTable(String tableName) {
        return getTables().get(tableName);
    }

    public DatabaseColumn getColumn(String tableName, String columnName) {
        DatabaseTable table = getTable(tableName);
        return table == null ? null : table.getColumns().get(columnName);
    }
}