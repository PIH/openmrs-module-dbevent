package org.openmrs.module.dbevent.consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.DbEvent;

/**
 * Simple event consumer that logs the event
 */
public class LoggingEventConsumer implements EventConsumer {

    private static final Logger log = LogManager.getLogger(LoggingEventConsumer.class);

    @Override
    public void accept(DbEvent event) {
        if (log.isInfoEnabled()) {
            log.info(event);
        }
    }
}
