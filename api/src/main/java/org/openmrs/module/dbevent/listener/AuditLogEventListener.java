package org.openmrs.module.dbevent.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.ThreadContext;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventListener;

public class AuditLogEventListener extends DbEventListener {

    private final Logger log = LogManager.getLogger(getClass());

    public static final Marker AUDIT_LOG_MARKER = MarkerManager.getMarker("AUDIT_LOG_MARKER");

    @Override
    public void processEvent(DbEvent dbEvent) {
        log.trace(AUDIT_LOG_MARKER, ThreadContext.getContext().toString());
    }
}
