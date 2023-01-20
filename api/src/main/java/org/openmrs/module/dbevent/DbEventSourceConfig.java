package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.Arrays;
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
        setProperty("offset.flush.timeout.ms", "15000");
        setProperty("include.schema.changes", "false");
        setProperty("database.server.id", Integer.toString(sourceId));
        setProperty("database.server.name", sourceName);
        setProperty("database.history", "io.debezium.relational.history.FileDatabaseHistory");
        setProperty("database.history.file.filename", schemaHistoryDataFile.getAbsolutePath());
        setProperty("decimal.handling.mode", "double");
        setProperty("tombstones.on.delete", "false");
        setProperty("snapshot.mode", "when_needed");

        // Initialize from runtime properties
        Properties runtimeProperties = context.getRuntimeProperties();
        setProperty("database.user", runtimeProperties.getProperty("connection.username"));
        setProperty("database.password", runtimeProperties.getProperty("connection.password"));
        String url = runtimeProperties.getProperty("connection.url");
        try {
            Driver driver = DriverManager.getDriver(url);
            for (DriverPropertyInfo driverPropertyInfo : driver.getPropertyInfo(url, null)) {
                switch (driverPropertyInfo.name.toLowerCase()) {
                    case "host": {
                        setProperty("database.hostname", driverPropertyInfo.value);
                        break;
                    }
                    case "port": {
                        setProperty("database.port", driverPropertyInfo.value);
                        break;
                    }
                    case "dbname": {
                        setProperty("database.dbname", driverPropertyInfo.value);
                        setProperty("database.include.list", driverPropertyInfo.value);
                        break;
                    }
                    default: break;
                }
            }
        }
        catch (Exception e) {
            throw new IllegalArgumentException("Invalid connection.url: " + url);
        }
    }

    /**
     * Provides a mechanism to add tables to include
     * @param tables the list of tables to include.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToInclude(String... tables) {
        if (tables != null && tables.length > 0) {
            String databaseName = config.getProperty("database.dbname");
            String tablePrefix = StringUtils.isNotBlank(databaseName) ? databaseName + "." : "";
            String tableConfig = Arrays.stream(tables)
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            config.setProperty("table.include.list", tableConfig);
        }
    }

    /**
     * Provides a mechanism to add tables to exclude
     * @param tables the list of tables to exclude.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToExclude(String... tables) {
        if (tables != null && tables.length > 0) {
            String databaseName = config.getProperty("database.dbname");
            String tablePrefix = StringUtils.isNotBlank(databaseName) ? databaseName + "." : "";
            String tableConfig = Arrays.stream(tables)
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            config.setProperty("table.exclude.list", tableConfig);
        }
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
