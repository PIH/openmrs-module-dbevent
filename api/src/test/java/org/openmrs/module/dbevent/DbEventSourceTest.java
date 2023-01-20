package org.openmrs.module.dbevent;

import org.junit.jupiter.api.Test;
import org.openmrs.module.dbevent.patient.PatientEventConsumer;
import org.openmrs.module.dbevent.test.Dataset;
import org.openmrs.module.dbevent.test.Mysql;
import org.openmrs.module.dbevent.test.TestEventContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DbEventSourceTest {

    @Test
    public void shouldConnectToMysql() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            Dataset dataset = mysql.executeQuery("select * from location");
            assertThat(dataset.size(), equalTo(9));
            assertThat(dataset.get(0).getInteger("location_id"), equalTo(1));
            assertThat(dataset.get(0).getString("name"), equalTo("Unknown Location"));
        }
    }

    @Test
    public void shouldStreamAndMonitorEvents() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            DbEventSource eventSource = new DbEventSource(new DbEventSourceConfig(100002,"EventLogger", ctx));
            eventSource.setEventConsumer(new LoggingEventConsumer());
            try {
                eventSource.start();
                for (int i=0; i<10; i++) {
                    Thread.sleep(1000);
                    if (i == 0 || i == 9) {
                        for (String attribute : DbEventLog.getMonitoringBeanAttributeNames("EventLogger")) {
                            Object value = DbEventLog.getMonitoringBeanAttribute("EventLogger", attribute);
                            System.out.println(attribute + ": " + value);
                        }
                    }
                }
            }
            finally {
                eventSource.stop();
            }
        }
    }

    @Test
    public void shouldTrackPatientChanges() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            mysql.executeUpdate("create table dbevent_patient (patient_id int primary key, last_updated datetime, deleted boolean)");
            EventContext ctx = new TestEventContext(mysql);
            DbEventSourceConfig config = new DbEventSourceConfig(100002,"EventLogger", ctx);
            config.configureTablesToInclude(
                    "patient", "patient_identifier",
                    "patient_program", "patient_state", "patient_program_attribute",
                    "person", "person_name", "person_address", "person_attribute", "relationship",
                    "visit", "visit_attribute",
                    "encounter", "encounter_provider", "encounter_diagnosis",
                    "obs", "allergy", "allergy_reaction", "conditions", "appointment", "condition",
                    "orders", "order_group", "test_order", "drug_order", "referral_order", "order_attribute"
            );
            DbEventSource eventSource = new DbEventSource(config);
            eventSource.setEventConsumer(new PatientEventConsumer());
            try {
                eventSource.start();
                Thread.sleep(1000*10);
                Dataset ds = mysql.executeQuery("select * from dbevent_patient");
                System.out.println(ds);
            }
            finally {
                eventSource.stop();
            }
        }
    }
}
