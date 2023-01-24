package org.openmrs.module.dbevent;

import org.apache.commons.dbutils.handlers.MapListHandler;
import org.junit.jupiter.api.Test;
import org.openmrs.module.dbevent.test.Mysql;
import org.openmrs.module.dbevent.test.TestEventContext;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            assertTrue(metadata.getTables().size() > 100);
            DatabaseTable visitTable = metadata.getTables().get("visit");
            assertNotNull(visitTable);
            assertThat(visitTable.getDatabaseName(), equalTo(metadata.getDatabaseName()));
            assertThat(visitTable.getTableName(), equalTo("visit"));
            assertThat(visitTable.getColumns().size(), equalTo(16));
            assertFalse(visitTable.getColumns().get("creator").isPrimaryKey());
            DatabaseColumn visitIdColumn = visitTable.getColumns().get("visit_id");
            assertNotNull(visitIdColumn);
            assertTrue(visitIdColumn.isPrimaryKey());
            assertThat(visitIdColumn.getReferencedBy().size(), equalTo(3));
            metadata.print();
        }
    }

    @Test
    public void shouldJoinToPatient() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
            List<DatabaseJoin> joins = metadata.getJoins("visit_attribute", "patient", false);
            assertThat(joins.size(), equalTo(2));
            assertThat(joins.get(0).getFromColumn().getTableName(), equalTo("visit_attribute"));
            assertThat(joins.get(0).getFromColumn().getColumnName(), equalTo("visit_id"));
            assertThat(joins.get(0).getToColumn().getTableName(), equalTo("visit"));
            assertThat(joins.get(0).getToColumn().getColumnName(), equalTo("visit_id"));
            assertThat(joins.get(1).getFromColumn().getTableName(), equalTo("visit"));
            assertThat(joins.get(1).getFromColumn().getColumnName(), equalTo("patient_id"));
            assertThat(joins.get(1).getToColumn().getTableName(), equalTo("patient"));
            assertThat(joins.get(1).getToColumn().getColumnName(), equalTo("patient_id"));

            assertJoins(metadata, "test_order", "patient", 2);
            assertJoins(metadata, "patient_identifier", "patient", 1);
            assertJoins(metadata, "patient_program", "patient", 1);
            assertJoins(metadata, "patient_state", "patient", 2);
            assertJoins(metadata, "patient_program_attribute", "patient", 2);
            assertJoins(metadata, "visit", "patient", 1);
            assertJoins(metadata, "visit_attribute", "patient", 2);
            assertJoins(metadata, "encounter", "patient", 1);
            assertJoins(metadata, "encounter_provider", "patient", 2);
            assertJoins(metadata, "note", "patient", 1);
            assertJoins(metadata, "obs", "patient", 0);
            assertJoins(metadata, "person", "patient", 0);
            assertJoins(metadata, "person_name", "patient", 0);
            assertJoins(metadata, "emr_radiology_order", "patient", 3);
        }
    }

    protected void assertJoins(DatabaseMetadata metadata, String fromTable, String toTable, int numJoins) {
        List<DatabaseJoin> l = metadata.getJoins(fromTable, toTable, false);
        assertThat(l.size(), equalTo(numJoins));
    }
}
