package org.openmrs.module.dbevent;

import liquibase.pro.packaged.C;
import org.junit.jupiter.api.Test;
import org.openmrs.module.dbevent.consumer.LoggingEventConsumer;
import org.openmrs.module.dbevent.test.Dataset;
import org.openmrs.module.dbevent.test.Mysql;
import org.openmrs.module.dbevent.test.TestContextWrapper;

import javax.management.MBeanInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class DbEventSourceTest {

    @Test
    public void shouldConnectToMysql() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            Dataset dataset = mysql.executeQuery("select * from location");
            assertThat(dataset.size(), equalTo(1));
            assertThat(dataset.get(0).getInteger("location_id"), equalTo(1));
            assertThat(dataset.get(0).getString("name"), equalTo("Unknown Location"));
        }
    }

    @Test
    public void shouldStreamEvents() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            ContextWrapper contextWrapper = new TestContextWrapper(mysql);
            DbEventSource eventSource = new DbEventSource(100002,"EventLogger", contextWrapper);
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
}
