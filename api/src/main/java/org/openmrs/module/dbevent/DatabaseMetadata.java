package org.openmrs.module.dbevent;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
     * Returns a List of DatabaseJoinPaths that represent the shortest distinct pathways that join back
     * to the specified column from the specified table, excluding any pathways that go through the tablesToExclude list
     * @param fromTable the table to start from
     * @param toColumn the column to naviagate to
     * @param tablesToExclude any pathways through these tables will be excluded
     * @return the List of DatabaseJoinPaths from the given table to the given column
     */
    public List<DatabaseJoinPath> getPathsToColumn(DatabaseTable fromTable, DatabaseColumn toColumn, List<String> tablesToExclude) {

        // Ensure the exclusions list includes the current table for this and recursive invocations
        List<String> exclusions = new ArrayList<>(tablesToExclude);
        exclusions.add(fromTable.getTableName());

        // Collect all join pathways
        List<DatabaseJoinPath> allPaths = new ArrayList<>();
        Integer minPathSize = null;
        for (DatabaseJoin join : fromTable.getForeignKeyReferences()) {
            if (!exclusions.contains(join.getPrimaryKey().getTableName())) {
                DatabaseJoinPath path = new DatabaseJoinPath();
                path.add(join);
                if (join.getPrimaryKey().equals(toColumn)) {
                    allPaths.add(path);
                    minPathSize = (minPathSize == null || minPathSize > path.size() ? path.size() : minPathSize);
                } else {
                    DatabaseTable refTable = getTable(join.getPrimaryKey().getTableName());
                    List<DatabaseJoinPath> pathsFromRef = getPathsToColumn(refTable, toColumn, exclusions);
                    for (DatabaseJoinPath pathFromRef : pathsFromRef) {
                        DatabaseJoinPath refPath = path.clone();
                        refPath.addAll(pathFromRef);
                        allPaths.add(refPath);
                        minPathSize = (minPathSize == null || minPathSize > refPath.size() ? refPath.size() : minPathSize);
                    }
                }
            }
        }

        // Identify if there are any non-nullable paths.  If so, return all that have the shortest length
        List<DatabaseJoinPath> shortestNonNullablePaths = new ArrayList<>();
        for (DatabaseJoinPath path : allPaths) {
            if (path.size() == minPathSize && !path.isNullable()) {
                shortestNonNullablePaths.add(path);
            }
        }

        if (!shortestNonNullablePaths.isEmpty()) {
            return shortestNonNullablePaths;
        }

        return allPaths;
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