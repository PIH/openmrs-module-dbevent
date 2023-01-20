package org.openmrs.module.dbevent.patient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.DbEvent;
import org.openmrs.module.dbevent.DbEventSourceConfig;
import org.openmrs.module.dbevent.EventConsumer;
import org.openmrs.module.dbevent.Operation;
import org.openmrs.module.dbevent.Rocks;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Consumes patient-related events
 */
public class PatientEventConsumer implements EventConsumer {

    private static final Logger log = LogManager.getLogger(PatientEventConsumer.class);

    private DbEventSourceConfig config;
    private Connection connection;
    private Rocks keyMap;
    private Rocks patientStatusMap;
    private int eventCount = 0;

    @Override
    public void startup(DbEventSourceConfig config) {
        try {
            this.config = config;
            connection = config.getContext().openConnection();
            keyMap = new Rocks(new File(config.getContext().getModuleDataDir(), "keyMap.db"));
            patientStatusMap = new Rocks(new File(config.getContext().getModuleDataDir(), "patientStatus.db"));
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred starting up", e);
        }
    }

    @Override
    public void shutdown() {
        config.getContext().closeConnection(connection);
        keyMap.close();
        patientStatusMap.close();
    }

    @Override
    public void accept(DbEvent event) {
        switch (event.getTable()) {
            case "patient":
            case "patient_identifier":
            case "patient_program":
            case "visit":
            case "encounter":
            case "orders":
            case "order_group":
            case "allergy":
            case "conditions":
            case "encounter_diagnosis":
            case "appointment":
            case "condition": {
                handlePatientEvent(event.getValues().getInteger("patient_id"), event);
                break;
            }
            case "person":
            case "person_name":
            case "person_attribute":
            case "person_address":
            case "obs": {
                handlePersonEvent(event.getValues().getInteger("person_id"), event);
                break;
            }
            case "relationship": {
                handlePersonEvent(event.getValues().getInteger("person_a"), event);
                handlePersonEvent(event.getValues().getInteger("person_b"), event);
                break;
            }
            case "patient_state":
            case "patient_program_attribute": {
                handlePatientProgramEvent(event.getValues().getInteger("patient_program_id"), event);
                break;
            }
            case "visit_attribute": {
                handleVisitEvent(event.getValues().getInteger("visit_id"), event);
                break;
            }
            case "encounter_provider": {
                handleEncounterEvent(event.getValues().getInteger("encounter_id"), event);
                break;
            }
            case "allergy_reaction": {
                handleAllergyEvent(event.getValues().getInteger("allergy_id"), event);
                break;
            }
            case "test_order":
            case "drug_order":
            case "referral_order":
            case "order_attribute":
                handleOrderEvent(event.getValues().getInteger("order_id"), event);
                break;
            default: {
                if (event.getValues().getInteger("patient_id") != null) {
                    handlePatientEvent(event.getValues().getInteger("patient_id"), event);
                }
                else if (event.getValues().getInteger("person_id") != null) {
                    handlePersonEvent(event.getValues().getInteger("person_id"), event);
                }
                else if (event.getValues().getInteger("patient_program_id") != null) {
                    handlePatientProgramEvent(event.getValues().getInteger("patient_program_id"), event);
                }
                else if (event.getValues().getInteger("visit_id") != null) {
                    handleVisitEvent(event.getValues().getInteger("visit_id"), event);
                }
                else if (event.getValues().getInteger("encounter_id") != null) {
                    handleEncounterEvent(event.getValues().getInteger("encounter_id"), event);
                }
                else if (event.getValues().getInteger("allergy_id") != null) {
                    handleAllergyEvent(event.getValues().getInteger("allergy_id"), event);
                }
                else if (event.getValues().getInteger("order_id") != null) {
                    handleOrderEvent(event.getValues().getInteger("order_id"), event);
                }
                else {
                    if (log.isTraceEnabled()) {
                        log.trace("Not logged as patient event: " + event);
                    }
                }
            }
        }
    }

    public void handlePatientEvent(Integer patientId, DbEvent event) {
        log.warn(++eventCount + " - " + event.getTable() + ": " + event.getKey());
        PatientStatus patientStatus = new PatientStatus();
        patientStatus.setPatientId(patientId);
        if (event.getOperation() == Operation.READ) {
            patientStatus.updateLastUpdatedIfLater(event.getValues().getLong("date_created"));
            patientStatus.updateLastUpdatedIfLater(event.getValues().getLong("date_changed"));
            patientStatus.updateLastUpdatedIfLater(event.getValues().getLong("date_voided"));
        }
        else {
            patientStatus.updateLastUpdatedIfLater(event.getTimestamp());
        }
        if (event.getTable().equals("patient")) {
            if (event.getOperation() == Operation.DELETE || event.getValues().getBoolean("voided")) {
                patientStatus.setDeleted(true);
            }
        }
        if (patientStatus.getLastUpdated() != null) {
            Timestamp lastUpdated = new Timestamp(patientStatus.getLastUpdated());
            boolean deleted = patientStatus.isDeleted();

            Object[] savedStatus = patientStatusMap.get(patientId);
            if (savedStatus == null || !savedStatus[0].equals(lastUpdated) || !savedStatus[1].equals(deleted)) {
                patientStatusMap.put(patientId, new Object[]{lastUpdated, deleted});
                executeUpdate(
                        "insert into dbevent_patient (patient_id, last_updated, deleted) values (?, ?, ?) on duplicate key update last_updated = ?, deleted = ?",
                        patientId, lastUpdated, deleted, lastUpdated, deleted
                );
            }
        }
        else {
            log.warn("Skipping event with no last updated date: " + event);
        }
    }

    public void handlePersonEvent(Integer personId, DbEvent event) {
        log.debug("Person " + personId + ": " + event);
        try {
            Integer patientId = getValue("patient_id", "patient", "patient_id", personId);
            handlePatientEvent(patientId, event);
        }
        catch (Exception e) {
            log.trace("No patient found for person: " + personId);
        }
    }

    public void handlePatientProgramEvent(Integer patientProgramId, DbEvent event) {
        log.debug("Patient Program " + patientProgramId + ": " + event);
        Integer patientId = getValue("patient_id", "patient_program", "patient_program_id", patientProgramId);
        handlePatientEvent(patientId, event);
    }

    public void handleVisitEvent(Integer visitId, DbEvent event) {
        log.debug("Visit " + visitId + ": " + event);
        Integer patientId = getValue("patient_id", "visit", "visit_id", visitId);
        handlePatientEvent(patientId, event);
    }

    public void handleEncounterEvent(Integer encounterId, DbEvent event) {
        log.debug("Encounter " + encounterId + ": " + event);
        Integer patientId = getValue("patient_id", "encounter", "encounter_id", encounterId);
        handlePatientEvent(patientId, event);
    }

    public void handleAllergyEvent(Integer allergyId, DbEvent event) {
        log.debug("Allergy " + allergyId + ": " + event);
        Integer patientId = getValue("patient_id", "allergy", "allergy_id", allergyId);
        handlePatientEvent(patientId, event);
    }

    public void handleOrderEvent(Integer orderId, DbEvent event) {
        log.debug("Order " + orderId + ": " + event);
        Integer patientId = getValue("patient_id", "orders", "order_id", orderId);
        handlePatientEvent(patientId, event);
    }

    private Integer getValue(String valCol, String table, String keyCol, Integer keyVal) {
        String rocksKey = table + "." + keyCol + ":" + keyVal;
        Integer val = keyMap.get(rocksKey);
        if (val == null) {
            String sql = "select " + valCol + " from " + table + " where " + keyCol + " = " + keyVal;
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (val != null) {
                            throw new RuntimeException("More than one value found for " + rocksKey);
                        }
                        val = rs.getInt(1);
                    }
                }
            }
            catch (Exception e) {
                throw new RuntimeException("Error executing SQL", e);
            }
            if (val != null) {
                keyMap.put(rocksKey, val);
            }
            else {
                throw new RuntimeException("Unable to find a value for " + rocksKey);
            }
        }
        return val;
    }

    private void executeUpdate(String statement, Object... values) {
        log.warn("Updating: " + Arrays.asList(values));
        try (PreparedStatement insertStmt = connection.prepareStatement(statement)) {
            for (int i=0; i<values.length; i++) {
                insertStmt.setObject(i+1, values[i]);
            }
            insertStmt.execute();
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred updating executing update", e);
        }
    }
}
