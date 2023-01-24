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

    public List<DatabaseJoin> getJoins(String fromTableName, String toTableName, boolean includeNullableColumns, String... excludedTables) {
        List<DatabaseJoin> ret = new ArrayList<>();
        List<String> exclusions = new ArrayList<>(Arrays.asList(excludedTables));
        if (!exclusions.contains(fromTableName)) {
            exclusions.add(fromTableName);
            DatabaseTable fromTable = getTables().get(fromTableName);
            for (DatabaseColumn fromColumn : fromTable.getColumns().values()) {
                for (DatabaseColumn toColumn : fromColumn.getReferences()) {
                    if (toColumn.getTableName().equals(toTableName)) {
                        if (includeNullableColumns || !toColumn.isNullable()) {
                            if (!exclusions.contains(toColumn.getTableName())) {
                                ret.add(new DatabaseJoin(fromColumn, toColumn));
                            }
                        }
                    }
                }
            }
            if (ret.isEmpty()) {
                for (DatabaseColumn c : fromTable.getColumns().values()) {
                    if (!c.getReferences().isEmpty() && (includeNullableColumns || !c.isNullable())) {
                        for (DatabaseColumn refColumn : c.getReferences()) {
                            if (!exclusions.contains(refColumn.getTableName())) {
                                excludedTables = exclusions.toArray(new String[0]);
                                List<DatabaseJoin> refs = getJoins(refColumn.getTableName(), toTableName, includeNullableColumns, excludedTables);
                                if (!refs.isEmpty()) {
                                    refs.add(0, new DatabaseJoin(c, refColumn));
                                    if (ret.isEmpty() || refs.size() < ret.size()) {
                                        ret = refs;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
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