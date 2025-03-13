package org.openmrs.module.dbevent.listener;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.BaseDbEventTest;
import org.openmrs.module.dbevent.DbEventContext;
import org.openmrs.module.dbevent.DbEventListenerConfig;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ExtendWith(MysqlExtension.class)
public class AuditLoggerTest extends BaseDbEventTest {

    public static final String SOURCE = "TEST_SOURCE";

    Logger logger = (Logger) LogManager.getLogger(AuditLogger.class);

    @Override
    protected String getPattern() {
        return "Marker:%marker | Operation:%X{operation} | Table:%X{table}";
    }

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        addMemoryAppenderToLogger(logger, Level.TRACE);
    }

    @AfterEach
    @Override
    public void teardown() {
        super.teardown();
    }

    @Test
    public void shouldLogLines() {
        DbEventContext ctx = MysqlExtension.getEventContext();
        DbEventListenerConfig config = new DbEventListenerConfig(100002, SOURCE, new Properties(), ctx);
        config.setProperty("debezium.snapshot.mode", "when_needed");
        config.configureTablesToInclude(Arrays.asList("location"));
        AuditLogger listener = new AuditLogger();
        try {
            listener.init(config);
            TestUtils.waitForNumberOfSnapshotEvents(SOURCE, 5);
        }
        finally {
            listener.stop();
        }
        int numberOfReads = Long.valueOf(listener.getStatus().getNumberOfReads()).intValue();
        int numLinesFound = 0;
        List<String> auditLog = memoryAppender.getLogLines();
        for (String line : auditLog) {
            if (line.startsWith("Marker:DBEVENT_AUDIT_LOG_MARKER")) {
                assertThat(line, equalTo("Marker:DBEVENT_AUDIT_LOG_MARKER | Operation:READ | Table:location"));
                numLinesFound++;
            }
        }
        assertThat(numLinesFound, greaterThanOrEqualTo(5));
        assertThat(numLinesFound, equalTo(numberOfReads));
    }
}
