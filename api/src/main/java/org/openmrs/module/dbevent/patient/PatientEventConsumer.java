package org.openmrs.module.dbevent.patient;

import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.DatabaseMetadata;
import org.openmrs.module.dbevent.DatabaseTable;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventSourceConfig;
import org.openmrs.module.dbevent.EventConsumer;
import org.openmrs.module.dbevent.Operation;
import org.openmrs.module.dbevent.Rocks;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Consumes patient-related events
 */
public class PatientEventConsumer implements EventConsumer {

    private static final Logger log = LogManager.getLogger(PatientEventConsumer.class);

    private final DbEventSourceConfig config;
    private Rocks keysDb;
    private Rocks statusDb;
    private Long snapshotTimestamp = null;

    public PatientEventConsumer(DbEventSourceConfig config) {
        this.config = config;
    }

    @Override
    public void startup() {
        try {
            keysDb = new Rocks(new File(config.getContext().getModuleDataDir(), "keys.db"));
            statusDb = new Rocks(new File(config.getContext().getModuleDataDir(), "status.db"));
            snapshotTimestamp = statusDb.get("snapshotTimestamp");
            if (snapshotTimestamp == null) {
                long timestamp = System.currentTimeMillis();
                performInitialSnapshot();
                snapshotTimestamp = timestamp;
                statusDb.put("snapshotTimestamp", snapshotTimestamp);
            }
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
        if (event.getOperation() == Operation.READ || event.getTimestamp() < snapshotTimestamp) {
            if (log.isTraceEnabled()) {
                log.trace("Skipping event: " + event);
            }
            return;
        }
        // patient, patient_identifier, patient_program, visit, encounter, orders, order_group
        // allergy, conditions, encounter_diagnosis, appointmentscheduling_appointment...
        if (event.getValues().getInteger("patient_id") != null) {
            handlePatientEvent(event.getValues().getInteger("patient_id"), event);
        }
        //  person, person_name, person_attribute, person_address, obs
        else if (event.getValues().getInteger("person_id") != null) {
            join(event, "person_id", "patient", "patient_id", false);
        }
        // relationship
        else if (event.getTable().equals("relationship")) {
            join(event, "person_a", "patient", "patient_id", false);
            join(event, "person_b", "patient", "patient_id", false);
        }
        // patient_state, patient_program_attribute
        else if (event.getValues().getInteger("patient_program_id") != null) {
            join(event, "patient_program_id", "patient_program", "patient_program_id", true);
        }
        // visit_attribute
        else if (event.getValues().getInteger("visit_id") != null) {
            join(event, "visit_id", "visit", "visit_id", true);
        }
        // encounter_provider
        else if (event.getValues().getInteger("encounter_id") != null) {
            join(event, "encounter_id", "encounter", "encounter_id", true);
        }
        // allergy_reaction
        else if (event.getValues().getInteger("allergy_id") != null) {
            join(event, "allergy_id", "allergy", "allergy_id", true);
        }
        // test_order, drug_order, referral_order, order_attribute
        else if (event.getValues().getInteger("order_id") != null) {
            join(event, "order_id", "orders", "order_id", true);
        }
        else {
            if (log.isTraceEnabled()) {
                log.trace("Not logged as patient event: " + event);
            }
        }
    }

    /**
     * Looks up the patient associated with the event and, if found, calls handlePatientEvent
     */
    protected void join(DbEvent event, String eventJoinColumn, String lookupTable, String lookupTableJoinColumn, boolean required) {
        Integer eventJoinValue = event.getValues().getInteger(eventJoinColumn);
        String rocksKey = lookupTable + "." + lookupTableJoinColumn + ":" + eventJoinValue;
        Integer patientId = keysDb.get(rocksKey);
        if (patientId == null) {
            String sql = "select patient_id from " + lookupTable + " where " + lookupTableJoinColumn + " = " + eventJoinValue;
            patientId = config.getContext().getDatabase().executeQuery(sql, new ScalarHandler<>(1));
            if (patientId != null) {
                keysDb.put(rocksKey, patientId);
            }
            else if (required) {
                throw new RuntimeException("Unable to find a value for " + rocksKey);
            }
            else {
                if (log.isTraceEnabled()) {
                    log.trace("Not able to join " + event + " with patient, skipping");
                }
            }
        }
        if (patientId != null) {
            handlePatientEvent(patientId, event);
        }
    }

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
    }

    /**
     * Rather than stream all initial READ events one by one, it is much more efficient to set up the initial state
     * by querying the database directly to record the most recent date updated and deleted status for all patients.
     * This
     */
    protected void performInitialSnapshot() {
        config.getContext().getDatabase().executeUpdate(
                "insert ignore into dbevent_patient (patient_id, last_updated, deleted) " +
                        "select patient_id, greatest(date_created, ifnull(date_changed, date_created), ifnull(date_voided, date_created)), voided from patient"
        );
        DatabaseMetadata metadata = config.getContext().getDatabase().getMetadata();
        for (DatabaseTable table : metadata.getTables().values()) {
            if (config.isIncluded(table) && !table.getTableName().equals("patient")) {
                Set<String> columns = table.getColumns().keySet();
                List<String> joinCols = new ArrayList<>();
                if (columns.contains("patient_id")) {
                    joinCols.add("patient_id");
                } else if (columns.contains("person_id")) {
                    joinCols.add("person_id");
                } else if (table.getTableName().equals("relationship")) {
                    joinCols.add("person_a");
                    joinCols.add("person_b");
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
                if (!joinCols.isEmpty() && !dateCols.isEmpty()) {
                    for (String joinCol : joinCols) {
                        config.getContext().getDatabase().executeUpdate("update dbevent_patient p " +
                                "inner join " + table.getTableName() + " x on p.patient_id = x." + joinCol + " " +
                                "set p.last_updated = greatest(p.last_updated, " + String.join(",", dateCols) + ")"
                        );
                    }
                }
            }
        }
    }
}
