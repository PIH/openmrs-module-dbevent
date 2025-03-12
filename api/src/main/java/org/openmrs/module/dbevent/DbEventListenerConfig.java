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
 * Represents the configuration for an instance of a DbEventListener
 * This configuration is made up of the following core components:
 * - sourceId is the unique identifier which is used by Debezium to track which change events the listener has processed
 *   This must be unique across all registered past and present listeners and must not change over time.
 * - sourceName is a more human-friendly name/description that corresponds to the given sourceId
 * - properties that control the debezium configuration and listener-specific configurations
 */
@Data
public class DbEventListenerConfig {

    public static final String MODULE_PREFIX = "dbevent.";
    public static final String DEBEZIUM_NAMESPACE = "debezium.";

    private final Integer sourceId;
    private final String sourceName;
    private final Properties config;
    private final DbEventContext context;

    public DbEventListenerConfig(Integer sourceId, String sourceName) {
        this(sourceId, sourceName, new DbEventContext());
    }

    public DbEventListenerConfig(Integer sourceId, String sourceName, DbEventContext context) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
        this.context = context;
        this.config = new Properties();
        File offsetsDataFile = new File(context.getModuleDataDir(), sourceId + "_offsets.dat");
        File schemaHistoryDataFile = new File(context.getModuleDataDir(), sourceId + "_schema_history.dat");

        setProperty("retryIntervalMillis", "60000"); // By default, set 1 minute as the retry interval

        // Initialize default values for source configuration.  The full list for MySQL connector properties is here:
        // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties
        setDebeziumProperty("name", sourceName);
        setDebeziumProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        setDebeziumProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        setDebeziumProperty("offset.storage.file.filename", offsetsDataFile.getAbsolutePath());
        setDebeziumProperty("offset.flush.interval.ms", "0");
        setDebeziumProperty("offset.flush.timeout.ms", "5000");
        setDebeziumProperty("include.schema.changes", "false");
        setDebeziumProperty("database.server.id", Integer.toString(sourceId));
        setDebeziumProperty("database.server.name", sourceName);
        setDebeziumProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        setDebeziumProperty("database.history.file.filename", schemaHistoryDataFile.getAbsolutePath());
        setDebeziumProperty("decimal.handling.mode", "double");
        setDebeziumProperty("tombstones.on.delete", "false");
        setDebeziumProperty("snapshot.mode", "schema_only");
        setDebeziumProperty("database.user", context.getDatabase().getUsername());
        setDebeziumProperty("database.password", context.getDatabase().getPassword());
        setDebeziumProperty("database.hostname", context.getDatabase().getHostname());
        setDebeziumProperty("database.port", context.getDatabase().getPort());
        setDebeziumProperty("database.dbname", context.getDatabase().getDatabaseName());
        setDebeziumProperty("database.include.list", context.getDatabase().getDatabaseName());
        
        for (String runtimePropertyName : context.getRuntimeProperties().stringPropertyNames()) {
            String sourcePrefix = MODULE_PREFIX + sourceId + ".";
            if (runtimePropertyName.toLowerCase().startsWith(sourcePrefix)) {
                String propertyName = runtimePropertyName.substring(sourcePrefix.length());
                setProperty(propertyName, context.getRuntimeProperties().getProperty(runtimePropertyName));
            }
        }
    }

    /**
     * Sets the configuration property with the given name to the given value
     */
    public void setProperty(String propertyName, String value) {
        config.setProperty(propertyName, value);
    }

    /**
     * Sets the configuration property for Debezium with the given name to the given value
     * If the name is not prefixed with "debezium.", then this prefix is added
     */
    public void setDebeziumProperty(String propertyName, String value) {
        if (!propertyName.toLowerCase().startsWith(DEBEZIUM_NAMESPACE)) {
            propertyName = DEBEZIUM_NAMESPACE + propertyName;
        }
        setProperty(propertyName, value);
    }

    /**
     * @return all configuration properties that used to configure debezium
     */
    public Properties getDebeziumProperties() {
        Properties p = new Properties();
        for (String propertyName : config.stringPropertyNames()) {
            if (propertyName.toLowerCase().startsWith(DEBEZIUM_NAMESPACE)) {
                p.setProperty(propertyName.substring(DEBEZIUM_NAMESPACE.length()), config.getProperty(propertyName));
            }
        }
        return p;
    }

    /**
     * @return the configured database name
     */
    public String getDatabaseName() {
        String ret = config.getProperty(DEBEZIUM_NAMESPACE + "database.dbname");
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
            setDebeziumProperty("table.include.list", tableConfig);
        }
    }

    /**
     * @return the configured table.include.list patterns
     */
    public List<String> getIncludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = config.getProperty(DEBEZIUM_NAMESPACE + "table.include.list");
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
            setDebeziumProperty("table.exclude.list", tableConfig);
        }
    }

    /**
     * @return the configured table.exclude.list patterns
     */
    public List<String> getExcludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = config.getProperty(DEBEZIUM_NAMESPACE + "table.exclude.list");
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
            return Integer.parseInt(config.getProperty("retryIntervalMillis"));
        }
        catch (Exception e) {
            return 60000;
        }
    }

    /**
     * @return true if this listener is enabled, which is true by default
     */
    public boolean isEnabled() {
        return Boolean.parseBoolean(config.getProperty("enabled", "true"));
    }

    /**
     * @return the currently configured offsets file
     */
    public File getOffsetsFile() {
        return new File(config.getProperty(DEBEZIUM_NAMESPACE + "offset.storage.file.filename"));
    }

    /**
     * @return the currently configured database schema history file
     */
    public File getDatabaseHistoryFile() {
        return new File(config.getProperty(DEBEZIUM_NAMESPACE + "database.history.file.filename"));
    }
}
