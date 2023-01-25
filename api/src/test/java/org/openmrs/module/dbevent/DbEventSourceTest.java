package org.openmrs.module.dbevent;

import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.patient.PatientEventConsumer;
import org.openmrs.module.dbevent.test.MysqlExtension;
import org.openmrs.module.dbevent.test.TestEventConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MysqlExtension.class)
public class DbEventSourceTest {

    @Test
    public void shouldStreamAndMonitorEvents() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DbEventSource eventSource = new DbEventSource(new DbEventSourceConfig(100002,"EventLogger", ctx));
        eventSource.setEventConsumer(new TestEventConsumer());
        try {
            eventSource.start();
            List<String> attNames = new ArrayList<>();
            for (int i=0; i<10; i++) {
                attNames = DbEventLog.getMonitoringBeanAttributeNames("EventLogger");
                if (attNames.isEmpty()) {
                    Thread.sleep(1000);
                }
                else {
                    i = 10;
                }
            }
            assertFalse(attNames.isEmpty());
            Object value = DbEventLog.getMonitoringBeanAttribute("EventLogger", "TotalTableCount");
            assertNotNull(value);
            assertTrue(Integer.parseInt(value.toString()) > 0);
        }
        finally {
            eventSource.stop();
        }
    }

    @Test
    public void shouldTrackPatientChanges() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        Database db = ctx.getDatabase();
        db.executeUpdate("create table dbevent_patient (patient_id int primary key, last_updated datetime, deleted boolean)");
        DbEventSourceConfig config = new DbEventSourceConfig(100002,"EventLogger", ctx);
        config.configureTablesToInclude(ctx.getDatabase().getMetadata().getPatientTableNames());
        DbEventSource eventSource = new DbEventSource(config);
        eventSource.setEventConsumer(new PatientEventConsumer(config));
        try {
            eventSource.start();
            List<Map<String, Object>> results = new ArrayList<>();
            for (int i=0; i<10; i++) {
                results = db.executeQuery("select * from dbevent_patient", new MapListHandler());
                if (results.isEmpty()) {
                    Thread.sleep(1000);
                }
                else {
                    i = 10;
                }
            }
            assertFalse(results.isEmpty());
            assertThat(results.get(0).get("patient_id"), equalTo(3));
            assertNotNull(results.get(0).get("last_updated"));
            assertFalse((Boolean)results.get(0).get("deleted"));
        }
        finally {
            db.executeUpdate("drop table dbevent_patient");
            eventSource.stop();
        }
    }
}
