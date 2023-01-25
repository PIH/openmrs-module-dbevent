package org.openmrs.module.dbevent;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.jupiter.api.Test;
import org.openmrs.module.dbevent.test.Mysql;
import org.openmrs.module.dbevent.test.TestEventContext;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        }
    }

    @Test
    public void shouldOpenAndCloseConnection() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            Database database = ctx.getDatabase();
            Connection connection = database.openConnection();
            assertFalse(connection.isClosed());
            QueryRunner qr = new QueryRunner();
            String sql = "select count(*) from location";
            Long numLocations = qr.query(connection, sql, new ScalarHandler<>(1));
            assertThat(numLocations, equalTo(9L));
            database.closeConnection(connection);
            assertTrue(connection.isClosed());
        }
    }

    @Test
    public void shouldGetMetadata() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            String query = "select count(*) from information_schema.tables where table_type = 'base table' AND table_schema = 'dbevent'";
            Long expectedNumTables = ctx.getDatabase().executeQuery(query, new ScalarHandler<>());
            DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
            assertEquals(expectedNumTables, metadata.getTables().size());
            DatabaseTable visitTable = metadata.getTables().get("visit");
            assertNotNull(visitTable);
            assertThat(visitTable.getDatabaseName(), equalTo(metadata.getDatabaseName()));
            assertThat(visitTable.getTableName(), equalTo("visit"));
            assertThat(visitTable.getColumns().size(), equalTo(16));
            assertFalse(visitTable.getColumns().get("creator").isPrimaryKey());
            DatabaseColumn visitIdColumn = visitTable.getColumns().get("visit_id");
            assertNotNull(visitIdColumn);
            assertTrue(visitIdColumn.isPrimaryKey());
            assertFalse(visitIdColumn.isNullable());
            assertThat(visitIdColumn.getReferencedBy().size(), equalTo(3));
            assertThat(visitIdColumn.getReferences().size(), equalTo(0));
            DatabaseColumn patientIdColumn = visitTable.getColumns().get("patient_id");
            assertNotNull(patientIdColumn);
            assertFalse(patientIdColumn.isPrimaryKey());
            assertFalse(patientIdColumn.isNullable());
            assertThat(patientIdColumn.getReferencedBy().size(), equalTo(0));
            assertThat(patientIdColumn.getReferences().size(), equalTo(1));
            DatabaseColumn changedByColumn = visitTable.getColumns().get("changed_by");
            assertNotNull(changedByColumn);
            assertFalse(changedByColumn.isPrimaryKey());
            assertTrue(changedByColumn.isNullable());
            assertThat(changedByColumn.getReferencedBy().size(), equalTo(0));
            assertThat(changedByColumn.getReferences().size(), equalTo(1));
            //metadata.print();
        }
    }

    @Test
    public void shouldExecuteQuery() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            Database database = ctx.getDatabase();
            String sql = "select location_id, name from location order by location_id";
            List<Map<String, Object>> dataset = database.executeQuery(sql, new MapListHandler());
            assertThat(dataset.size(), equalTo(9));
            assertThat(dataset.get(0).get("location_id"), equalTo(1));
            assertThat(dataset.get(0).get("name"), equalTo("Unknown Location"));
        }
    }

    @Test
    public void shouldExecuteUpdate() throws Exception {
        try (Mysql mysql = Mysql.open()) {
            EventContext ctx = new TestEventContext(mysql);
            Database database = ctx.getDatabase();
            String sql = "update location set name = ? where location_id = ?";
            database.executeUpdate(sql, "TESTING", 1);
            sql = "select name from location where location_id = ?";
            String value = database.executeQuery(sql, new ScalarHandler<>(1), 1);
            assertThat(value, equalTo("TESTING"));
        }
    }
}
