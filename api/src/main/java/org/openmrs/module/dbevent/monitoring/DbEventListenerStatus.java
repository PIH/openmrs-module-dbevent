package org.openmrs.module.dbevent.monitoring;

import lombok.Data;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventListener;

import java.util.Map;
import java.util.TreeMap;

/**
 * MBean interface to support jmx monitoring of a DbEventListener
 */
@Data
public class DbEventListenerStatus implements DbEventListenerStatusMXBean {

    private Integer id;
    private String name;
    private String status;
    private Long latestEventTime;
    private String latestEvent;
    private boolean latestEventProcessed = false;
    private String latestEventErrorMessage;
    private Long latestEventErrorRetryNum;
    private Map<String, Long> eventsProcessedByTable = new TreeMap<>();

    public void initialize(DbEventListener listener) {
        setId(listener.getId());
        setName(listener.getName());
        setStatus("INITIALIZING");
    }

    public void started() {
        setStatus("STARTED");
    }

    public void processingStarted(DbEvent dbEvent) {
        setStatus("PROCESSING");
        setLatestEventTime(dbEvent.getTimestamp());
        setLatestEvent(dbEvent.toString());
    }

    public void processingCompleted(DbEvent dbEvent) {
        setStatus("OK");
        setLatestEventProcessed(true);
        eventsProcessedByTable.compute(dbEvent.getTable(), (k, v) -> v == null ? 1 : v + 1);
        setLatestEventErrorMessage(null);
        setLatestEventErrorRetryNum(null);
    }

    public void processingFailed(Throwable e) {
        setStatus("ERROR");
        setLatestEventProcessed(false);
        setLatestEventErrorMessage(e.getMessage());
        setLatestEventErrorRetryNum(latestEventErrorRetryNum == null ? 1 : (latestEventErrorRetryNum + 1));
    }
}
