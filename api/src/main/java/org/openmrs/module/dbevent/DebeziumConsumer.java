package org.openmrs.module.dbevent;

import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Implementation of a Debezium ChangeEvent consumer, which abstracts the Debezium API behind a DbEvent
 * and ensures that the registered DbEvent EventConsumer is successfully processed before moving onto the next
 * record, with a configurable retryInterval upon failure.
 */
public class DebeziumConsumer implements Consumer<ChangeEvent<SourceRecord, SourceRecord>> {

    private static final Logger log = LogManager.getLogger(DebeziumConsumer.class);

    private final DbEventListener eventListener;
    private boolean stopped = false;

    public DebeziumConsumer(DbEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * This the primary handler for all Debezium-generated change events.  Per the
     * <a href="https://debezium.io/documentation/reference/stable/development/engine.html">Debezium Documentation</a>
     * this function should not throw any exceptions, as these will simply get logged and Debezium will continue onto
     * the next source record.  So if any exception is caught, this logs the Exception, and retries again after
     * a configurable retryInterval, until it passes.  This effectively blocks any subsequent processing.
      * @param changeEvent the Debeziumn generated event to process
     */
    @Override
    public final void accept(ChangeEvent<SourceRecord, SourceRecord> changeEvent) {
        if (stopped) {
            throw new RuntimeException("The Debezium consumer has been stopped prior to processing: " + changeEvent);
        }
        try {
            DbEvent event = new DbEvent(changeEvent);
            eventListener.getStatus().processingStarted(event);
            eventListener.accept(event);
            eventListener.getStatus().processingCompleted(event);
        }
        catch (Throwable e) {
            eventListener.getStatus().processingFailed(e);
            log.error("An error occurred processing change event: {}: {}", changeEvent, e.getMessage());
            log.debug(e);
            try {
                log.debug("Retrying in {} ms", eventListener.getConfig().getRetryIntervalMillis());
                TimeUnit.MILLISECONDS.sleep(eventListener.getConfig().getRetryIntervalMillis());
            }
            catch (Exception e2) {
                log.error("An exception occurred while waiting to retry processing change event", e2);
            }
            accept(changeEvent);
        }
    }

    public void start() {
        this.stopped = false;
    }

    public void stop() {
        this.stopped = true;
    }
}
