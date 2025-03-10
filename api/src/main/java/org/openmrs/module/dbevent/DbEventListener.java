package org.openmrs.module.dbevent;

import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.ThreadContext;
import org.openmrs.module.dbevent.monitoring.DbEventListenerStatus;
import org.openmrs.module.dbevent.monitoring.DbEventMonitor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * This source emits DbEvents from a configured database
 */
public abstract class DbEventListener implements Consumer<DbEvent> {

    private final Logger log = LogManager.getLogger(getClass());
    public static final Marker EVENT_MARKER = MarkerManager.getMarker("DB_EVENT");

    @Getter
    private final Integer id;

    @Getter
    private final String name;

    @Getter
    private final DbEventListenerConfig config;

    @Getter
    private DbEventListenerStatus status;

    private DebeziumConsumer debeziumConsumer;

    private ExecutorService executor;

    private DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;

    /**
     * Id must be unique across all other DbEventListeners (and any other Debezium clients or mysql replication nodes)
     */
    public DbEventListener(Integer id) {
        this.id = id;
        this.name = getClass().getSimpleName();
        this.config = new DbEventListenerConfig(this.id, this.name, new DbEventContext());
    }

    /**
     * Constructor that allows full configuration
     */
    public DbEventListener(DbEventListenerConfig config) {
        this.id = config.getSourceId();
        this.name = config.getSourceName();
        this.config = config;
    }

    /**
     * Subclasses should implement this method to perform any steps necessary prior to receiving events
     * For example, if some sort of initial state needs to be computed before receiving streaming changes, that can be added here.
     */
    public void beforeProcessingEvents() {}

    /**
     * Subclasses should implement this method to process each DbChange event
     */
    public abstract void processEvent(DbEvent event);

    /**
     * Allows for resetting the source.  This deletes any existing history and offset files
     */
    public void reset() {
        log.info("Resetting Event Source: {}", config.getSourceId());
        FileUtils.deleteQuietly(config.getOffsetsFile());
        FileUtils.deleteQuietly(config.getDatabaseHistoryFile());
    }

    /**
     * This is the actual method that is implemented for each DbEvent.
     * Subclasses should typically implement processEvent rather than override this method
     */
    @Override
    public final void accept(DbEvent dbEvent) {
        logEvent(dbEvent);
        processEvent(dbEvent);
    }

    /**
     * Starts up the event source to stream events from the database and consume by the registered consumer
     */
    public void start() {
        log.info("Starting {}: {} - {}", getClass().getSimpleName(), config.getSourceId(), config.getSourceName());
        log.trace("{} - configuration: {}", config.getSourceName(), config);
        log.trace("{} - monitoring tables: {}", config.getSourceName(), config.getMonitoredTables());

        if (config.getOffsetsFile().getParentFile().mkdirs()) {
            log.info("Created directory: {}", config.getOffsetsFile().getParentFile());
        }
        if (config.getDatabaseHistoryFile().getParentFile().mkdirs()) {
            log.info("Created directory: {}", config.getOffsetsFile().getParentFile());
        }

        getStatus().initialize(this);
        DbEventMonitor.registerMonitoringBean(this);

        debeziumConsumer = new DebeziumConsumer(this);
        beforeProcessingEvents();
        engine = DebeziumEngine.create(Connect.class)
                .using(config.getDebeziumConfig())
                .notifying(debeziumConsumer)
                .build();

        log.info("{} - starting debezium execution engine", config.getSourceName());
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        getStatus().started();
    }

    public DbEventListenerStatus getStatus() {
        if (status == null) {
            status = new DbEventListenerStatus();
            status.setId(getId());
            status.setName(getName());
        }
        return status;
    }

    /**
     * Trace logs the given event for the given EVENT_MARKER.
     * This allows logging configurations that would support comprehensive audit logging as appropriates
     */
    public synchronized void logEvent(DbEvent event) {
        if (log.isTraceEnabled()) {
            try {
                ThreadContext.put("timestamp", event.getTimestamp().toString());
                ThreadContext.put("sourceName", event.getSourceName());
                ThreadContext.put("table", event.getTable());
                ThreadContext.put("operation", event.getOperation().name());
                ThreadContext.put("key", event.getKey().toString());
                ThreadContext.put("before", event.getBefore().toString());
                ThreadContext.put("after", event.getAfter().toString());
                ThreadContext.put("source", event.getSource().toString());
                log.trace(EVENT_MARKER, ThreadContext.getContext().toString());
            }
            finally {
                ThreadContext.clearAll();
            }
        }
    }

    /**
     * Stops the event source
     */
    public void stop() {
        log.info("Stopping {}: {} - {}", getClass().getSimpleName(), config.getSourceId(), config.getSourceName());
        debeziumConsumer.stop();

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
