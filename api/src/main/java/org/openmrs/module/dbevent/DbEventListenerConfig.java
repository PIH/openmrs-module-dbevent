package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    private final Logger log = LogManager.getLogger(getClass());

    public static final String MODULE_PREFIX = "dbevent.";
    public static final String DEBEZIUM_NAMESPACE = "debezium.";

    private final Integer sourceId;
    private final String sourceName;
    private final Properties debeziumProperties;
    private DbEventContext context;
    private Integer retryIntervalMillis = 60000;
    private boolean enabled = true;

    public DbEventListenerConfig(Integer sourceId, String sourceName) {
        this(sourceId, sourceName, new DbEventContext());
    }

    public DbEventListenerConfig(Integer sourceId, String sourceName, DbEventContext context) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
        this.debeziumProperties = new Properties();
        this.context = context;

        File offsetsDataFile = new File(getDataDirectory(), "debezium_offsets.dat");
        File schemaHistoryDataFile = new File(getDataDirectory(), "debezium_schema_history.dat");

        // First set default values for configuration.  Debezium MySQL connector properties can be found here:
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
    }

    /**
     * @return the data directory that should be used to for this listener to record processing information
     */
    public File getDataDirectory() {
        return new File(context.getModuleDataDir(), sourceId.toString());
    }

    /**
     * Sets the configuration property for Debezium with the given name to the given value
     * If the name is prefixed with "debezium.", then this prefix is stripped
     */
    public void setDebeziumProperty(String propertyName, String value) {
        if (propertyName.toLowerCase().startsWith(DEBEZIUM_NAMESPACE)) {
            propertyName = propertyName.substring(DEBEZIUM_NAMESPACE.length());
        }
        debeziumProperties.setProperty(propertyName, value);
    }

    /**
     * @return the configured debezium properties, including any defaults and any overrides from runtime properties
     */
    public Properties getDebeziumProperties() {
        Properties p = new Properties();
        p.putAll(debeziumProperties);
        String prefix = MODULE_PREFIX + sourceName + "." + DEBEZIUM_NAMESPACE;
        for (String runtimePropertyName : context.getRuntimeProperties().stringPropertyNames()) {
            if (runtimePropertyName.startsWith(prefix)) {
                String debeziumPropertyName = runtimePropertyName.substring(prefix.length());
                p.setProperty(debeziumPropertyName, context.getRuntimeProperties().getProperty(runtimePropertyName));
            }
        }
        return p;
    }

    /**
     * @return the configured database name
     */
    public String getDatabaseName() {
        String ret = debeziumProperties.getProperty("database.dbname");
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
        String val = debeziumProperties.getProperty("table.include.list");
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
        String val = debeziumProperties.getProperty("table.exclude.list");
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
        return getIntegerRuntimePropertyValue("retryIntervalMillis", retryIntervalMillis);
    }

    /**
     * @return true if this listener is enabled, which is true by default
     */
    public boolean isEnabled() {
        return getBooleanRuntimePropertyValue("enabled", enabled);
    }

    /**
     * @return the currently configured offsets file
     */
    public File getOffsetsFile() {
        return new File(debeziumProperties.getProperty("offset.storage.file.filename"));
    }

    /**
     * @return the currently configured database schema history file
     */
    public File getDatabaseHistoryFile() {
        return new File(debeziumProperties.getProperty("database.history.file.filename"));
    }

    /**
     * @return the runtime property value configured for this particular sourceId
     */
    public String getRuntimePropertyValue(String property) {
        return context.getRuntimeProperties().getProperty(MODULE_PREFIX + sourceId + "." + property);
    }

    /**
     * @return a runtime property value configured for this DbEvent sourceId parsed into an Integer
     */
    public Integer getIntegerRuntimePropertyValue(String property, Integer defaultValue) {
        String value = getRuntimePropertyValue(property);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid integer configuration for " + property + "=" + value);
        }
    }

    /**
     * @return a runtime property value configured for this DbEvent sourceId parsed into a boolean
     */
    public boolean getBooleanRuntimePropertyValue(String property, boolean defaultValue) {
        String value = getRuntimePropertyValue(property);
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Boolean.parseBoolean(value);
        }
        catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid boolean configuration for " + property + "=" + value);
        }
    }
}
