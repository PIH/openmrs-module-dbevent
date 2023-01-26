package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Configuration of a DbEventSource
 */
@Data
public class DbEventSourceConfig {

    private static final Logger log = LogManager.getLogger(DbEventSourceConfig.class);

    private final Integer sourceId;
    private final String sourceName;
    private final Properties config;
    private final EventContext context;
    private Integer retryIntervalSeconds = 60; // By default, retry every minute on error

    public DbEventSourceConfig(Integer sourceId, String sourceName, EventContext context) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
        this.context = context;
        this.config = new Properties();
        File offsetsDataFile = new File(context.getModuleDataDir(), sourceId + "_offsets.dat");
        File schemaHistoryDataFile = new File(context.getModuleDataDir(), sourceId + "_schema_history.dat");

        // Initialize default values for source configuration.  The full list for MySQL connector properties is here:
        // https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-connector-properties
        setProperty("name", sourceName);
        setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        setProperty("offset.storage.file.filename", offsetsDataFile.getAbsolutePath());
        setProperty("offset.flush.interval.ms", "0");
        setProperty("offset.flush.timeout.ms", "5000");
        setProperty("include.schema.changes", "false");
        setProperty("database.server.id", Integer.toString(sourceId));
        setProperty("database.server.name", sourceName);
        setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        setProperty("database.history.file.filename", schemaHistoryDataFile.getAbsolutePath());
        setProperty("decimal.handling.mode", "double");
        setProperty("tombstones.on.delete", "false");
        setProperty("snapshot.mode", "when_needed");
        setProperty("database.user", context.getDatabase().getUsername());
        setProperty("database.password", context.getDatabase().getPassword());
        setProperty("database.hostname", context.getDatabase().getHostname());
        setProperty("database.port", context.getDatabase().getPort());
        setProperty("database.dbname", context.getDatabase().getDatabaseName());
        setProperty("database.include.list", context.getDatabase().getDatabaseName());
    }

    /**
     * @return the configured database name
     */
    public String getDatabaseName() {
        String ret = config.getProperty("database.dbname");
        return ret == null ? null : ret.trim();
    }

    /**
     * Provides a mechanism to add tables to include
     * @param tables the list of tables to include.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToInclude(Collection<String> tables) {
        if (tables != null && tables.size() > 0) {
            String tablePrefix = StringUtils.isNotBlank(getDatabaseName()) ? getDatabaseName() + "." : "";
            String tableConfig = tables.stream()
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            config.setProperty("table.include.list", tableConfig);
        }
    }

    /**
     * @return the configured table.include.list patterns
     */
    public List<String> getIncludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = config.getProperty("table.include.list");
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
        if (tables != null && tables.size() > 0) {
            String tablePrefix = StringUtils.isNotBlank(getDatabaseName()) ? getDatabaseName() + "." : "";
            String tableConfig = tables.stream()
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            config.setProperty("table.exclude.list", tableConfig);
        }
    }

    /**
     * @return the configured table.exclude.list patterns
     */
    public List<String> getExcludedTablePatterns() {
        List<String> ret = new ArrayList<>();
        String val = config.getProperty("table.exclude.list");
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
     * @param key the property to lookup
     * @return the configuration property with the given key
     */
    public String getProperty(String key) {
        return config.getProperty(key);
    }

    /**
     * This sets a configuration property with the given key and value
     * @param key the key to set
     * @param value the value to set
     */
    public void setProperty(String key, String value) {
        config.setProperty(key, value);
    }

    /**
     * @return the currently configured offsets file
     */
    public File getOffsetsFile() {
        return new File(config.getProperty("offset.storage.file.filename"));
    }

    /**
     * @return the currently configured database schema history file
     */
    public File getDatabaseHistoryFile() {
        return new File(config.getProperty("database.history.file.filename"));
    }
}
