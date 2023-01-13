package org.openmrs.module.dbevent;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.consumer.EventConsumer;

import java.io.File;
import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This source emits DbEvents from a configured database
 */
@Data
public class DbEventSource {

    private static final Logger log = LogManager.getLogger(DbEventSource.class);

    private final Integer sourceId;
    private final String sourceName;
    private final Properties config;
    private final ContextWrapper context;
    private EventConsumer eventConsumer;
    private Integer retryIntervalSeconds = 60; // By default, retry every minute on error

    @Getter(AccessLevel.PRIVATE)
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Getter(AccessLevel.PRIVATE)
    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

    /**
     * Contructor for a new EventSource
     * @param sourceId - a unique, identifiable name for this source
     */
    public DbEventSource(Integer sourceId, String sourceName, ContextWrapper context) {
        this.sourceId = sourceId;
        this.sourceName = sourceName;
        this.context = context;
        this.config = new Properties();
        File dataDirectory = new File(context.getApplicationDataDir(), "dbevent");
        File offsetsDataFile = new File(dataDirectory, sourceId + "_offsets.dat");
        File schemaHistoryDataFile = new File(dataDirectory, sourceId + "_schema_history.dat");

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
                        setProperty("table.include.list", driverPropertyInfo.value + ".*");
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
     * Provides a mechanism to configure the tables to include
     * @param tables the list of tables to include.  If not prefixed with a database name, it will be added
     */
    public void configureTablesToInclude(List<String> tables) {
        if (tables != null && !tables.isEmpty()) {
            String databaseName = config.getProperty("database.dbname");
            String tablePrefix = StringUtils.isNotBlank(databaseName) ? databaseName + "." : "";
            String tableConfig = tables.stream()
                    .map(t -> t.startsWith(tablePrefix) ? t : tablePrefix + t)
                    .collect(Collectors.joining(","));
            config.setProperty("table.include.list", tableConfig);
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

    /**
     * Allows for resetting the source.  This deletes any existing history and offset files.
     */
    public void reset() {
        log.info("Resetting Event Source: " + sourceId);
        FileUtils.deleteQuietly(getOffsetsFile());
        FileUtils.deleteQuietly(getDatabaseHistoryFile());
    }

    /**
     * Starts up the event source to stream events from the database and consume by the registered consumer
     */
    public void start() {
        log.info("Starting Event Source: " + sourceId);
        log.debug("Configuration: " + config);

        if (getOffsetsFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + getOffsetsFile().getParentFile());
        }
        if (getDatabaseHistoryFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + getOffsetsFile().getParentFile());
        }

        log.info("Starting event consumer: " + eventConsumer);
        eventConsumer.startup();

        engine = DebeziumEngine.create(Connect.class)
                .using(config)
                .notifying(new DebeziumConsumer(eventConsumer, retryIntervalSeconds))
                .build();

        log.info("Starting execution engine");
        executor.execute(engine);
    }

    /**
     * Stops the event source
     */
    public void stop() {
        log.info("Stopping Event Source: " + sourceId);
        try {
            log.info("Stopping event consumer: " + eventConsumer);
            eventConsumer.shutdown();
        }
        catch (Exception e) {
            log.warn("Error shutting down event consumer", e);
        }
        try {
            if (engine != null) {
                log.info("Closing execution engine");
                engine.close();
            }
        }
        catch (IOException e) {
            log.warn("An error occurred while attempting to close the engine", e);
        }
        try {
            log.info("Shutting down executor");
            executor.shutdown();
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                log.info("Waiting another 5 seconds for the embedded engine to shut down");
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
