package org.openmrs.module.dbevent.monitoring;

import java.util.Map;

/**
 * MBean interface to support jmx monitoring of a DbEventListener
 */
public interface DbEventListenerStatusMXBean {
    Integer getId();
    String getName();
    String getStatus();
    Long getLatestEventTime();
    String getLatestEvent();
    boolean isLatestEventProcessed();
    String getLatestEventErrorMessage();
    Long getLatestEventErrorRetryNum();
    Map<String, Long> getEventsProcessedByTable();
}
