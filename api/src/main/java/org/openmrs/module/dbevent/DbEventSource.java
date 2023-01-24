package org.openmrs.module.dbevent;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This source emits DbEvents from a configured database
 */
@Data
public class DbEventSource {

    private static final Logger log = LogManager.getLogger(DbEventSource.class);

    private final DbEventSourceConfig config;
    private EventConsumer eventConsumer;

    @Getter(AccessLevel.PRIVATE)
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    @Getter(AccessLevel.PRIVATE)
    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

    /**
     * Allows for resetting the source.  This deletes any existing history and offset files.
     */
    public void reset() {
        log.info("Resetting Event Source: " + config.getSourceId());
        FileUtils.deleteQuietly(config.getOffsetsFile());
        FileUtils.deleteQuietly(config.getDatabaseHistoryFile());
    }

    /**
     * Starts up the event source to stream events from the database and consume by the registered consumer
     */
    public void start() {
        log.info("Starting Event Source: " + config.getSourceId() + " - " + config.getSourceName());
        log.warn(config.getSourceName() + " - monitoring tables: " + config.getMonitoredTables());
        log.debug(config.getSourceName() + " - configuration: " + config);

        if (config.getOffsetsFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + config.getOffsetsFile().getParentFile());
        }
        if (config.getDatabaseHistoryFile().getParentFile().mkdirs()) {
            log.info("Created directory: " + config.getOffsetsFile().getParentFile());
        }

        log.info("Starting event consumer: " + eventConsumer);
        eventConsumer.startup();

        engine = DebeziumEngine.create(Connect.class)
                .using(config.getConfig())
                .notifying(new DebeziumConsumer(eventConsumer, config))
                .build();

        log.info("Starting execution engine");
        executor.execute(engine);
    }

    /**
     * Stops the event source
     */
    public void stop() {
        log.info("Stopping Event Source: " + config.getSourceId());
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
