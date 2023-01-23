package org.openmrs.module.dbevent;

import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.jupiter.api.Test;
import org.openmrs.module.dbevent.test.Mysql;
import org.openmrs.module.dbevent.test.TestEventContext;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DatabaseTest {

    @Test
    public void shouldConfigureFromProperties() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            Database database = ctx.getDatabase();
            assertNotNull(database);
            assertThat(database.getDatabaseName(), equalTo(Mysql.DATABASE_NAME));
            assertThat(database.getUsername(), equalTo("root"));
            assertNotNull(database.getPassword());
            assertNotNull(database.getHostname());
            assertNotNull(database.getPort());

            List<Map<String, Object>> dataset = database.executeQuery("select location_id, name from location order by location_id", new MapListHandler());
            assertThat(dataset.size(), equalTo(9));
            assertThat(dataset.get(0).get("location_id"), equalTo(1));
            assertThat(dataset.get(0).get("name"), equalTo("Unknown Location"));
        }
    }

    @Test
    public void shouldGetMetadata() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
            assertThat(metadata.getTables().size(), equalTo(187));
            DatabaseTable visitTable = metadata.getTables().get("visit");
            assertNotNull(visitTable);
            assertThat(visitTable.getDatabaseName(), equalTo(metadata.getDatabaseName()));
            assertThat(visitTable.getTableName(), equalTo("visit"));
            assertThat(visitTable.getColumns().size(), equalTo(16));
        }
    }
}
