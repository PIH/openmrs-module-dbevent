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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
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
    private Rocks keysDb;
    private Rocks statusDb;
    private Long maxEventTimestamp = null;

    public PatientEventConsumer(DbEventSourceConfig config) {
        this.config = config;
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
        } else {
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

    protected Long getMaxEventTimestamp() {
        LocalDateTime datetime = config.getContext().getDatabase().executeQuery(
                "select max(last_updated) from dbevent_patient", new ScalarHandler<>(1)
        );
        return (datetime == null ? null : ZonedDateTime.of(datetime, ZoneId.systemDefault()).toInstant().toEpochMilli());
    }
}
