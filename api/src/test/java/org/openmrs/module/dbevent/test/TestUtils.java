package org.openmrs.module.dbevent.test;

import org.openmrs.module.dbevent.DbEventLog;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Testing utility methods
 */
public class TestUtils {

    private static final long SLEEP_INTERVAL = 1000;

    public static void waitForSnapshotToStart(String sourceName) {
        boolean completed = false;
        while (!completed) {
            completed = !getSnapshotAttributes(sourceName).isEmpty();
            sleep(SLEEP_INTERVAL);
        }
    }

    public static void waitForSnapshotToComplete(String sourceName) {
        boolean completed = false;
        while (!completed) {
            completed = (Boolean) getSnapshotAttributes(sourceName).get("SnapshotCompleted");
            sleep(SLEEP_INTERVAL);
        }
    }

    public static void waitForNumberOfSnapshotEvents(String sourceName, int numberToWaitFor) {
        long num = 0;
        while (num < numberToWaitFor) {
            Long seen = (Long) getSnapshotAttributes(sourceName).get("TotalNumberOfEventsSeen");
            num = (seen == null ? 0 : seen);
            sleep(SLEEP_INTERVAL);
        }
    }

    public static void waitForNumberOfStreamingEvents(String sourceName, int numberToWaitFor) {
        long num = 0;
        while (num < numberToWaitFor) {
            Long seen = (Long) getStreamingAttributes(sourceName).get("TotalNumberOfEventsSeen");
            num = (seen == null ? 0 : seen);
            sleep(SLEEP_INTERVAL);
        }
    }

    public static Map<String, Object> getSnapshotAttributes(String sourceName) {
        try {
            return DbEventLog.getSnapshotMonitoringAttributes(sourceName);
        }
        catch (Exception e) {
            return new HashMap<>();
        }
    }

    public static Map<String, Object> getStreamingAttributes(String sourceName) {
        try {
            return DbEventLog.getStreamingMonitoringAttributes(sourceName);
        }
        catch (Exception e) {
            return new HashMap<>();
        }
    }

    public static void sleep(long millis) {
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        }
        catch (Exception e) {}
    }
}
