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

    /**
     * Convenience method to add a table to the metadata
     * @param table the table to add
     */
    public void addTable(DatabaseTable table) {
        tables.put(table.getTableName(), table);
    }

    /**
     * Returns all tables that directly reference the given table name, or indirectly through nested references
     * For example, getting all tables referenced by "patient" would include direct references such as
     * encounter (encounter.patient_id) and also indirect references such as encounter_provider, due to the fact
     * that patient is referenced by encounter, and encounter is referenced by encounter_provider.  Tables can be
     * excluded from this by passing them into the excludedTables array.  Excluding a table will exclude both the
     * given table and prevent any further tables referenced by the excluded table from being included unless they
     * are referenced directly or by another non-excluded table.
     * @param tableName the table for which to return references
     * @param excludedTables tables which should be excluded from the returned references
     * @return all tables directly or indirectly referenced by the given table, but not excluded
     */
    public Set<String> getTablesWithReferencesTo(String tableName, String... excludedTables) {
        Set<String> ret = new TreeSet<>();
        DatabaseTable table = tables.get(tableName);
        if (table != null) {
            ret.addAll(table.getTablesReferencedBy());
            Arrays.asList(excludedTables).forEach(ret::remove);
            List<String> nestedTables = new ArrayList<>(ret);
            for (String nestedTable : nestedTables) {
                List<String> nestedExclusions = new ArrayList<>(Arrays.asList(excludedTables));
                nestedExclusions.addAll(ret);
                ret.addAll(getTablesWithReferencesTo(nestedTable, nestedExclusions.toArray(new String[0])));
            }
        }
        return ret;
    }

    /**
     * Returns all tables related to patient data in the system.  This is based on all tables with person-related
     * data but which is not user-specific or provider-specific data.
     * @return all tables that relate to a patient
     * @see DatabaseMetadata#getTablesWithReferencesTo(String, String...)
     */
    public Set<String> getPatientTableNames() {
        String[] excludedTables = {"users", "provider"};
        Set<String> ret = new TreeSet<>(getTablesWithReferencesTo("person", excludedTables));
        ret.add("person");
        return ret;
    }

    /**
     * @param tableName the table to retrieve from the metadata
     * @return the DatabaseTable with the given name
     */
    public DatabaseTable getTable(String tableName) {
        return getTables().get(tableName);
    }

    /**
     * @param tableName the table name of the column to retrieve
     * @param columnName the column name of the column to retrieve
     * @return the DatabaseColumn with the given name in the given table
     */
    public DatabaseColumn getColumn(String tableName, String columnName) {
        DatabaseTable table = getTable(tableName);
        return table == null ? null : table.getColumns().get(columnName);
    }
}