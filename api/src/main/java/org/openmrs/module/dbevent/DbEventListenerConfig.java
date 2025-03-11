package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.openmrs.module.dbevent.database.DatabaseTable;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Represents configuration of Debezium for a particular DbEventListener
 */
@Data
public class DbEventListenerConfig {

    private final Integer sourceId;
    private final String sourceName;
    private final Properties listenerConfig;
    private final Properties debeziumConfig;
    private final DbEventContext context;

    public DbEventListenerConfig(Integer sourceId, String sourceName) {
        this(sourceId, sourceName, new DbEventContext());
    }

    public DbEventListenerConfig(Integer sourceId, String sourceName, DbEventContext context) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
        this.context = context;
        this.listenerConfig = new Properties();
        this.debeziumConfig = new Properties();
        File offsetsDataFile = new File(context.getModuleDataDir(), sourceId + "_offsets.dat");
        File schemaHistoryDataFile = new File(context.getModuleDataDir(), sourceId + "_schema_history.dat");

        listenerConfig.setProperty("retryIntervalMillis", "60000"); // By default, set 1 minute as the retry interval

        // Initialize default values for source configuration.  The full list for MySQL connector properties is here:
        // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties
        debeziumConfig.setProperty("name", sourceName);
        debeziumConfig.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        debeziumConfig.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        debeziumConfig.setProperty("offset.storage.file.filename", offsetsDataFile.getAbsolutePath());
        debeziumConfig.setProperty("offset.flush.interval.ms", "0");
        debeziumConfig.setProperty("offset.flush.timeout.ms", "5000");
        debeziumConfig.setProperty("include.schema.changes", "false");
        debeziumConfig.setProperty("database.server.id", Integer.toString(sourceId));
        debeziumConfig.setProperty("database.server.name", sourceName);
        debeziumConfig.setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        debeziumConfig.setProperty("database.history.file.filename", schemaHistoryDataFile.getAbsolutePath());
        debeziumConfig.setProperty("decimal.handling.mode", "double");
        debeziumConfig.setProperty("tombstones.on.delete", "false");
        debeziumConfig.setProperty("snapshot.mode", "schema_only");
        debeziumConfig.setProperty("database.user", context.getDatabase().getUsername());
        debeziumConfig.setProperty("database.password", context.getDatabase().getPassword());
        debeziumConfig.setProperty("database.hostname", context.getDatabase().getHostname());
        debeziumConfig.setProperty("database.port", context.getDatabase().getPort());
        debeziumConfig.setProperty("database.dbname", context.getDatabase().getDatabaseName());
        debeziumConfig.setProperty("database.include.list", context.getDatabase().getDatabaseName());
        
        for (String runtimePropertyName : context.getRuntimeProperties().stringPropertyNames()) {
            String sourcePrefix = "dbevent." + sourceId + ".";
            if (runtimePropertyName.toLowerCase().startsWith(sourcePrefix)) {
                setProperty(runtimePropertyName, context.getRuntimeProperties().getProperty(runtimePropertyName));
            }
        }
    }

    /**
     * Set a configuration property to a specific value
     * Properties that are prefixed with "dbevent." will have this prefix stripped
     * Properties that are prefixed with "dbevent.debezium." or "debezium." will have these prefixes stripped before setting as Debezium config
     * Properties that are not prefixed with "dbevent.debezium." or "debezium." are not added to the Debezium config,
     * but are used for non-Debezium-specific configuration of the listener
     */
    public void setProperty(String key, String value) {
        String sourcePrefix = "dbevent." + sourceId + ".";
        String debeziumPrefix = "debezium.";
        if (key.toLowerCase().startsWith(sourcePrefix)) {
            key = key.substring(sourcePrefix.length());
        }
        if (key.startsWith(debeziumPrefix)) {
            String debeziumPropertyName = key.substring(debeziumPrefix.length());
            debeziumConfig.setProperty(debeziumPropertyName, value);
        }
        else {
            listenerConfig.setProperty(key, value);
        }
    }

    /**
     * @return the configured database name
     */
    public String getDatabaseName() {
        String ret = debeziumConfig.getProperty("database.dbname");
        return ret == null ? null : ret.trim();
    }

    /**
     * Provides a mechanism to add tables to include
     * @param tables the list of tables to include.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToInclude(Collection<String> tables) {
        if (tables != null && !tables.isEmpty()) {
            String tablePrefix = StringUtils.isNotBlank(getDatabaseName()) ? getDatabaseName() + "." : "";
            String tableConfig = tables.stream()
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            debeziumConfig.setProperty("table.include.list", tableConfig);
        }
    }

    /**
     * @return the configured table.include.list patterns
     */
    public List<String> getIncludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = debeziumConfig.getProperty("table.include.list");
        if (val != null) {
            for (String tableName : val.split(",")) {
                ret.add(tableName.trim());
            }
        }
        return ret;
    }

    /**
     * Provides a mechanism to add tables to exclude
     * @param tables the list of tables to exclude.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToExclude(Collection<String> tables) {
        if (tables != null && !tables.isEmpty()) {
            String tablePrefix = StringUtils.isNotBlank(getDatabaseName()) ? getDatabaseName() + "." : "";
            String tableConfig = tables.stream()
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            debeziumConfig.setProperty("table.exclude.list", tableConfig);
        }
    }

    /**
     * @return the configured table.exclude.list patterns
     */
    public List<String> getExcludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = debeziumConfig.getProperty("table.exclude.list");
        if (val != null) {
            for (String tableName : val.split(",")) {
                ret.add(tableName.trim());
            }
        }
        return ret;
    }

    /**
     * @param table the table to check
     * @return true if the table is included in the configuration
     */
    public boolean isIncluded(DatabaseTable table) {
        String name = table.getDatabaseName() + "." + table.getTableName();
        List<String> includePatterns = getIncludedTablePatterns();
        if (!includePatterns.isEmpty()) {
            for (String pattern : includePatterns) {
                if (name.matches(pattern)) {
                    return true;
                }
            }
            return false;
        }
        else {
            List<String> excludePatterns = getExcludedTablePatterns();
            if (!excludePatterns.isEmpty()) {
                for (String pattern : excludePatterns) {
                    if (name.matches(pattern)) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    /**
     * @return all tables in the database that are included based on the included and excluded table configuration
     */
    public List<DatabaseTable> getMonitoredTables() {
        List<DatabaseTable> ret = new ArrayList<>();
        for (DatabaseTable table : context.getDatabase().getMetadata().getTables().values()) {
            if (isIncluded(table)) {
                ret.add(table);
            }
        }
        return ret;
    }

    /**
     * @return the configured retry interval in milliseconds, in case of a processing error;  defaults to 1 minute
     */
    public int getRetryIntervalMillis() {
        try {
            return Integer.parseInt(listenerConfig.getProperty("retryIntervalMillis"));
        }
        catch (Exception e) {
            return 60000;
        }
    }

    /**
     * @return true if this listener is enabled, which is true by default
     */
    public boolean isEnabled() {
        return Boolean.parseBoolean(listenerConfig.getProperty("enabled", "true"));
    }

    /**
     * @return the currently configured offsets file
     */
    public File getOffsetsFile() {
        return new File(debeziumConfig.getProperty("offset.storage.file.filename"));
    }

    /**
     * @return the currently configured database schema history file
     */
    public File getDatabaseHistoryFile() {
        return new File(debeziumConfig.getProperty("database.history.file.filename"));
    }
}
