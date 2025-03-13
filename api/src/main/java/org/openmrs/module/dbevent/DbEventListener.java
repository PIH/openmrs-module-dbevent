package org.openmrs.module.dbevent;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.monitoring.DbEventListenerStatus;
import org.openmrs.module.dbevent.monitoring.DbEventMonitor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * The DbEventListener class is intended to be the superclass of all Listener implementations that wish
 * to listen for particular database events and process these in some way.
 */
public abstract class DbEventListener implements Consumer<DbEvent> {

    private final Logger log = LogManager.getLogger(getClass());

    @Getter
    private DbEventListenerConfig config;

    private DbEventListenerStatus status;

    private DebeziumConsumer debeziumConsumer;

    private ExecutorService executor;

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

    /**
     * Default constructor
     */
    public DbEventListener() {
    }

    /**
     * This is the primary mechanism by which listeners are intended to be initialized to start executing in OpenMRS
     * It is expected that downstream consumers will create an instance of a given Listener class, and call the init
     * method with a particular configuration to start it up
     */
    public void init(DbEventListenerConfig config) {
        this.config = config;
        if (config == null || config.getSourceId() == null) {
            throw new IllegalArgumentException("The config must have a valid source id");
        }
        if (config.isEnabled()) {
            start();
        }
        else {
            log.warn("{} is disabled", getClass().getSimpleName());
        }
    }

    /**
     * Subclasses should implement this method to perform any steps necessary prior to receiving events
     * For example, if some sort of initial state needs to be computed before receiving streaming changes, that can be added here.
     */
    public void beforeProcessingEvents() {}

    /**
     * This is the primary method in this class that subclasses are expected to implement to process a particular DbEvent
     */
    public abstract void processEvent(DbEvent event);

    /**
     * Allows for resetting the source.  This deletes any existing history and offset files
     */
    public void reset() {
        log.info("Resetting Event Listener: {}", config.getSourceId());
        if (config.getDataDirectory().exists()) {
            if (!FileUtils.deleteQuietly(config.getDataDirectory())) {
                throw new IllegalStateException("Error resetting listener. Unable to delete data directory at: " + config.getDataDirectory());
            }
        }
    }

    /**
     * This is the actual method that is executed for each DbEvent.
     * Subclasses should implement processEvent rather than override this method
     */
    @Override
    public final void accept(DbEvent event) {
        processEvent(event);
    }

    /**
     * Starts up the event source to stream events from the database and consume by the registered consumer
     */
    public void start() {
        log.info("Starting {}: {} - {}", getClass().getSimpleName(), config.getSourceId(), config.getSourceName());
        log.trace("{} - configuration: {}", config.getSourceName(), config);
        log.trace("{} - monitoring tables: {}", config.getSourceName(), config.getMonitoredTables());

        if (config.getDataDirectory().mkdirs()) {
            log.info("Created data directory: {}", config.getDataDirectory());
        }
        if (config.getOffsetsFile().getParentFile().mkdirs()) {
            log.info("Created offsets file directory: {}", config.getOffsetsFile().getParentFile());
        }
        if (config.getDatabaseHistoryFile().getParentFile().mkdirs()) {
            log.info("Created database history file directory: {}", config.getOffsetsFile().getParentFile());
        }

        getStatus().initialize(this);
        DbEventMonitor.registerMonitoringBeans(this);

        debeziumConsumer = new DebeziumConsumer(this);
        beforeProcessingEvents();
        engine = DebeziumEngine.create(Connect.class)
                .using(config.getDebeziumProperties())
                .notifying(debeziumConsumer)
                .build();

        log.info("{} - starting debezium execution engine", config.getSourceName());
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        getStatus().started();
    }

    /**
     * Get the status of the listener
     */
    public DbEventListenerStatus getStatus() {
        if (status == null) {
            status = new DbEventListenerStatus();
            status.setId(getConfig().getSourceId());
            status.setName(getConfig().getSourceName());
        }
        return status;
    }

    /**
     * Stops this Listener from processing any further events
     */
    public void stop() {
        log.info("Stopping {}: {} - {}", getClass().getSimpleName(), config.getSourceId(), config.getSourceName());

        DbEventMonitor.unregisterMonitoringBeans(this);

        if (debeziumConsumer != null) {
            debeziumConsumer.stop();
        }

        try {
            if (engine != null) {
                log.info("{} - stopping debezium execution engine", config.getSourceName());
                engine.close();
            }
        }
        catch (IOException e) {
            log.warn("An error occurred while attempting to close the engine", e);
        }

        log.info("{} - stopping execution", config.getSourceName());
        try {
            if (executor != null) {
                executor.shutdown();
                while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.info("{} - Waiting another 5 seconds for the Debezium engine to shut down", config.getSourceName());
                }
            }
            executor = null;
        }
        catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
        }
    }
}
