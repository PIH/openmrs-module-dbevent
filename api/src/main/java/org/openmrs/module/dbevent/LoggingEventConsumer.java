package org.openmrs.module.dbevent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
