package org.openmrs.module.dbevent;

import io.debezium.engine.ChangeEvent;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.consumer.EventConsumer;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class DebeziumConsumer implements Consumer<ChangeEvent<SourceRecord, SourceRecord>> {

    private static final Logger log = LogManager.getLogger(DebeziumConsumer.class);

    private final EventConsumer eventConsumer;
    private final Integer retryIntervalSeconds;

    public DebeziumConsumer(EventConsumer eventConsumer, Integer retryIntervalSeconds) {
        this.eventConsumer = eventConsumer;
        this.retryIntervalSeconds = retryIntervalSeconds;
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
        try {
            DbEvent event = new DbEvent(changeEvent);
            eventConsumer.accept(event);
        }
        catch (Throwable e) {
            log.error("An error occurred processing change event: " + changeEvent  + ". Retrying in 1 minute", e);
            try {
                TimeUnit.SECONDS.sleep(retryIntervalSeconds);
            }
            catch (Exception e2) {
                log.error("An exception occurred while waiting to retry processing change event", e2);
            }
            accept(changeEvent);
        }
    }
}
