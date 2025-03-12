package org.openmrs.module.dbevent.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.ThreadContext;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventListener;

/**
 * Simple DbEventListener implementation that can be used to audit all changes via log4j
 * For example, a configuration like this would log to an audit.log file:
 *
 * <Appenders>
 *   ...
 *   <File name="AUDIT_LOG" fileName="${openmrs:applicationDirectory}/audit.log" append="true">
 * 	   <PatternLayout pattern="%d{ISO8601} | %X{operation} | %X{table} | %X{key}%n" />
 * 	 </File>
 * 	 ...
 * 	 <Loggers>
 * 	     ...
 * 	    <Logger name="org.openmrs.module.dbevent.listener.AuditLogEventListener" level="TRACE" additivity="false">
 * 	      <AppenderRef ref="AUDIT_LOG">
 * 	        <MarkerFilter marker="DBEVENT_AUDIT_LOG_MARKER"/>
 * 		  </AppenderRef>
 * 		</Logger>
 * 	     ...
 * 	 </Loggers>
 * </Appenders>
 *
 *
 */
public class AuditLogger extends DbEventListener {

    private final Logger log = LogManager.getLogger(getClass());

    public static final Marker AUDIT_LOG_MARKER = MarkerManager.getMarker("DBEVENT_AUDIT_LOG_MARKER");

    @Override
    public void processEvent(DbEvent dbEvent) {
        log.trace(AUDIT_LOG_MARKER, ThreadContext.getContext().toString());
    }
}
