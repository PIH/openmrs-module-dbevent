package org.openmrs.module.dbevent.monitoring;

import lombok.Data;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventListener;
import org.openmrs.module.dbevent.Operation;

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
    private long numberOfEvents = 0;
    private long numberOfReads = 0;
    private long numberOfInserts = 0;
    private long numberOfUpdates = 0;
    private long numberOfDeletes = 0;

    public void initialize(DbEventListener listener) {
        setId(listener.getConfig().getSourceId());
        setName(listener.getConfig().getSourceName());
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
        setLatestEventErrorMessage(null);
        setLatestEventErrorRetryNum(null);
        Operation operation = dbEvent.getOperation();
        numberOfEvents++;
        if (operation == Operation.READ) {
            numberOfReads++;
        }
        else if (operation == Operation.INSERT) {
            numberOfInserts++;
        }
        else if (operation == Operation.UPDATE) {
            numberOfUpdates++;
        }
        else if (operation == Operation.DELETE) {
            numberOfDeletes++;
        }
    }

    public void processingFailed(Throwable e) {
        setStatus("ERROR");
        setLatestEventProcessed(false);
        setLatestEventErrorMessage(e.getMessage());
        setLatestEventErrorRetryNum(latestEventErrorRetryNum == null ? 1 : (latestEventErrorRetryNum + 1));
    }
}
