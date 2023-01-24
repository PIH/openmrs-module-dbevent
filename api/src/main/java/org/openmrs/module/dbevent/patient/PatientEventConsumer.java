package org.openmrs.module.dbevent.patient;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.Patient;
import org.openmrs.module.dbevent.DatabaseJoin;
import org.openmrs.module.dbevent.DatabaseMetadata;
import org.openmrs.module.dbevent.DatabaseTable;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventSourceConfig;
import org.openmrs.module.dbevent.EventConsumer;
import org.openmrs.module.dbevent.Operation;
import org.openmrs.module.dbevent.Rocks;

import java.io.File;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Consumes patient-related events
 * For performance reasons on larger databases, this does not use a Debezium-generated snapshot, but rather
 * computes a custom snapshot that is then updated as needed from change events.
 * When used, this should be configured with "snapshot.mode = schema_only"
 */
public class PatientEventConsumer implements EventConsumer {

    private static final Logger log = LogManager.getLogger(PatientEventConsumer.class);

    private final DbEventSourceConfig config;
    private final Map<String, PatientIdQuery> patientIdQueries;
    private Rocks keysDb;
    private Rocks statusDb;
    private Long maxEventTimestamp = null;

    public PatientEventConsumer(DbEventSourceConfig config) {
        this.config = config;
        patientIdQueries = getPatientIdQueries();
    }

    @Override
    public void startup() {
        try {
            keysDb = new Rocks(new File(config.getContext().getModuleDataDir(), "keys.db"));
            statusDb = new Rocks(new File(config.getContext().getModuleDataDir(), "status.db"));
            Long snapshotTimestamp = statusDb.get("snapshotTimestamp");
            if (snapshotTimestamp == null) {
                long timestamp = System.currentTimeMillis();
                performInitialSnapshot();
                snapshotTimestamp = timestamp;
                statusDb.put("snapshotTimestamp", snapshotTimestamp);
            }
            maxEventTimestamp = getMaxEventTimestamp();
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred starting up", e);
        }
    }

    @Override
    public void shutdown() {
        keysDb.close();
        statusDb.close();
    }

    @Override
    public void accept(DbEvent event) {
        if (maxEventTimestamp != null && event.getTimestamp() < maxEventTimestamp) {
            if (log.isTraceEnabled()) {
                log.trace("Skipping event prior to " + maxEventTimestamp + ": " + event);
            }
            return;
        }
        Integer patientId = event.getValues().getInteger("patient_id");
        if (patientId == null) {
            String rocksKey = event.getTable() + "." + event.getKey() + ":patient_id";
            patientId = keysDb.get(rocksKey);
            if (patientId == null) {
                PatientIdQuery query = patientIdQueries.get(event.getTable());
                if (query != null) {
                    Object lookupVal = event.getValues().get(query.getValueColumn());
                    if (lookupVal != null) {
                        patientId = config.getContext().getDatabase().executeQuery(query.getSql(), new ScalarHandler<>(1), lookupVal);
                    }
                }
                if (patientId != null) {
                    keysDb.put(rocksKey, patientId);
                }
                else {
                    log.warn("No patient found for event, skipping.  Event = " + event);
                }
            }
        }
        if (patientId != null) {
            handlePatientEvent(patientId, event);
        }
    }

    /**
     * Once a particular event is connected ot a particular patient, this tracks this change for the patient
     * @param patientId the patient id related to the event
     * @param event the event to track
     */
    public void handlePatientEvent(Integer patientId, DbEvent event) {
        if (log.isDebugEnabled()) {
            log.debug("Patient Event: " + patientId + " - " + event);
        }
        Timestamp lastUpdated = new Timestamp(event.getTimestamp());
        String sql = "insert into dbevent_patient (patient_id, last_updated) values (?, ?) on duplicate key update last_updated = ?";
        config.getContext().getDatabase().executeUpdate(sql, patientId, lastUpdated, lastUpdated);
        if (event.getTable().equals("patient")) {
            if (event.getOperation() == Operation.DELETE || event.getValues().getBoolean("voided")) {
                sql = "update dbevent_patient set deleted = true where patient_id = ? and deleted = false";
                config.getContext().getDatabase().executeUpdate(sql, patientId);
            }
            else if (!event.getValues().getBoolean("voided")) {
                sql = "update dbevent_patient set deleted = false where patient_id = ? and deleted = true";
                config.getContext().getDatabase().executeUpdate(sql, patientId);
            }
        }
        maxEventTimestamp = event.getTimestamp();
    }

    /**
     * Rather than stream all initial READ events one by one, it is much more efficient to set up the initial state
     * by querying the database directly to record the most recent date updated and deleted status for all patients.
     * This method performs this operation, and returns the max last_updated in the database
     */
    protected void performInitialSnapshot() {
        config.getContext().getDatabase().executeUpdate(
                "insert ignore into dbevent_patient (patient_id, last_updated, deleted) " +
                        "select patient_id, greatest(date_created, ifnull(date_changed, date_created), ifnull(date_voided, date_created)), voided from patient"
        );
        DatabaseMetadata metadata = config.getContext().getDatabase().getMetadata();
        for (DatabaseTable table : metadata.getTables().values()) {
            String tableName = table.getTableName();
            if (config.isIncluded(table) && !tableName.equals("patient")) {
                Set<String> columns = table.getColumns().keySet();
                List<String> joins = new ArrayList<>();
                if (columns.contains("patient_id")) {
                    joins.add("inner join " + tableName + " x on p.patient_id = x.patient_id");
                } else if (columns.contains("person_id")) {
                    joins.add("inner join " + tableName + " x on p.patient_id = x.person_id");
                } else if (table.getTableName().equals("relationship")) {
                    joins.add("inner join relationship x on p.patient_id = x.person_a");
                    joins.add("inner join relationship x on p.patient_id = x.person_b");
                }
                else if (columns.contains("patient_program_id")) {
                    joins.add("inner join patient_program pp on p.patient_id = pp.patient_id inner join " + tableName + " x on x.patient_program_id = pp.patient_program_id");
                }
                else if (columns.contains("visit_id")) {
                    joins.add("inner join visit v on p.patient_id = v.patient_id inner join " + tableName + " x on x.visit_id = v.visit_id");
                }
                else if (columns.contains("encounter_id")) {
                    joins.add("inner join encounter e on p.patient_id = e.patient_id inner join " + tableName + " x on x.encounter_id = e.encounter_id");
                }
                else if (columns.contains("allergy_id")) {
                    joins.add("inner join allergy a on p.patient_id = a.patient_id inner join " + tableName + " x on x.allergy_id = a.allergy_id");
                }
                else if (columns.contains("order_id")) {
                    joins.add("inner join orders o on p.patient_id = o.patient_id inner join " + tableName + " x on x.order_id = o.order_id");
                }
                List<String> dateCols = new ArrayList<>();
                if (columns.contains("date_created")) {
                    dateCols.add("ifnull(x.date_created, p.last_updated)");
                }
                if (columns.contains("date_changed")) {
                    dateCols.add("ifnull(x.date_changed, p.last_updated)");
                }
                if (columns.contains("date_voided")) {
                    dateCols.add("ifnull(x.date_voided, p.last_updated)");
                }
                if (!joins.isEmpty() && !dateCols.isEmpty()) {
                    for (String join : joins) {
                        config.getContext().getDatabase().executeUpdate("update dbevent_patient p " + join + " " +
                                "set p.last_updated = greatest(p.last_updated, " + String.join(",", dateCols) + ")"
                        );
                    }
                }
            }
        }
    }

    public Map<String, PatientIdQuery> getPatientIdQueries() {
        Map<String, PatientIdQuery> ret = new LinkedHashMap<>();
        DatabaseMetadata metadata = config.getContext().getDatabase().getMetadata();
        for (DatabaseTable table : config.getMonitoredTables()) {
            String tableName = table.getTableName();
            List<DatabaseJoin> joins = metadata.getJoins(tableName, "patient", false, "users", "provider");
            if (!joins.isEmpty()) {
                StringBuilder sql = new StringBuilder();
                DatabaseJoin firstJoin = joins.get(0);
                DatabaseJoin lastJoin = joins.get(joins.size()-1);
                sql.append("select ").append(lastJoin.getFromColumn().getTableAndColumn());
                for (int i = 0; i < joins.size()-1; i++) {
                    DatabaseJoin join = joins.get(i);
                    String fromTable = join.getFromColumn().getTableName();
                    String fromColumn = join.getFromColumn().getColumnName();
                    String toTable = join.getToColumn().getTableName();
                    String toColumn = join.getToColumn().getColumnName();
                    if (i == 0) {
                        sql.append(" from ").append(fromTable).append(" ").append(fromTable);
                    }
                    sql.append(" inner join ").append(toTable).append(" ").append(toTable);
                    sql.append(" on ").append(fromTable).append(".").append(fromColumn);
                    sql.append(" = ").append(toTable).append(".").append(toColumn);
                }
                sql.append(" where ").append(firstJoin.getFromColumn().getTableAndColumn()).append(" = ?");
                ret.put(table.getTableName(), new PatientIdQuery(firstJoin.getFromColumn().getColumnName(), sql.toString()));
            }
            else {
                joins = metadata.getJoins(tableName, "person", false, "users", "provider");
                if (!joins.isEmpty()) {
                    StringBuilder sql = new StringBuilder();
                    DatabaseJoin firstJoin = joins.get(0);
                    DatabaseJoin lastJoin = joins.get(joins.size()-1);
                    sql.append("select ").append(lastJoin.getFromColumn().getTableAndColumn());
                    for (int i = 0; i < joins.size()-1; i++) {
                        DatabaseJoin join = joins.get(i);
                        String fromTable = join.getFromColumn().getTableName();
                        String fromColumn = join.getFromColumn().getColumnName();
                        String toTable = join.getToColumn().getTableName();
                        String toColumn = join.getToColumn().getColumnName();
                        if (i == 0) {
                            sql.append(" from ").append(fromTable).append(" ").append(fromTable);
                        }
                        sql.append(" inner join ").append(toTable).append(" ").append(toTable);
                        sql.append(" on ").append(fromTable).append(".").append(fromColumn);
                        sql.append(" = ").append(toTable).append(".").append(toColumn);
                    }
                    sql.append(" inner join patient patient on person.person_id = patient.patient_id ");
                    sql.append(" where ").append(firstJoin.getFromColumn().getTableAndColumn()).append(" = ?");
                    ret.put(table.getTableName(), new PatientIdQuery(firstJoin.getFromColumn().getColumnName(), sql.toString()));
                }
            }
        }
        return ret;
    }

    protected Long getMaxEventTimestamp() {
        LocalDateTime datetime = config.getContext().getDatabase().executeQuery(
                "select max(last_updated) from dbevent_patient", new ScalarHandler<>(1)
        );
        return (datetime == null ? null : ZonedDateTime.of(datetime, ZoneId.systemDefault()).toInstant().toEpochMilli());
    }

    @Data
    @AllArgsConstructor
    public static class PatientIdQuery {
        private String valueColumn;
        private String sql;

        @Override
        public String toString() {
            return sql.replace("?", ":" + valueColumn);
        }
    }
}
