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

    private final DbEventSourceConfig eventSourceConfig;
    private final EventConsumer eventConsumer;
    private boolean stopped = false;

    public DebeziumConsumer(EventConsumer eventConsumer, DbEventSourceConfig eventSourceConfig) {
        this.eventConsumer = eventConsumer;
        this.eventSourceConfig = eventSourceConfig;
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
        DbEventStatus status = null;
        if (stopped) {
            throw new RuntimeException("The Debezium consumer has been stopped prior to processing: " + changeEvent);
        }
        try {
            DbEvent event = new DbEvent(changeEvent);
            status = DbEventLog.log(event);
            eventConsumer.accept(event);
            status.setProcessed(true);
        }
        catch (Throwable e) {
            log.error("An error occurred processing change event: " + changeEvent  + ". Retrying in 1 minute", e);
            if (status != null) {
                status.setError(e);
            }
            try {
                TimeUnit.SECONDS.sleep(eventSourceConfig.getRetryIntervalSeconds());
            }
            catch (Exception e2) {
                log.error("An exception occurred while waiting to retry processing change event", e2);
            }
            accept(changeEvent);
        }
    }

    public void cancel() {
        this.stopped = true;
    }
}
