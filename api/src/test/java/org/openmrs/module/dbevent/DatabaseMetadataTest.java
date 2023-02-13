package org.openmrs.module.dbevent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.test.MysqlExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MysqlExtension.class)
public class DatabaseMetadataTest {

    private static final Logger log = LogManager.getLogger(DatabaseMetadataTest.class);

    public static final String[] EXPECTED_TABLES = {
            "address_hierarchy_address_to_entry_map",
            "allergy",
            "allergy_reaction",
            "appointmentscheduling_appointment",
            "appointmentscheduling_appointment_request",
            "appointmentscheduling_appointment_status_history",
            "cohort_member",
            "concept_proposal",
            "concept_proposal_tag_map",
            "conditions",
            "diagnosis_attribute",
            "drug_order",
            "emr_radiology_order",
            "encounter",
            "encounter_diagnosis",
            "encounter_provider",
            "fhir_diagnostic_report",
            "fhir_diagnostic_report_performers",
            "fhir_diagnostic_report_results",
            "logic_rule_token",
            "logic_rule_token_tag",
            "name_phonetics",
            "note",
            "obs",
            "order_attribute",
            "order_group",
            "order_group_attribute",
            "orders",
            "paperrecord_paper_record",
            "paperrecord_paper_record_merge_request",
            "paperrecord_paper_record_request",
            "patient",
            "patient_identifier",
            "patient_program",
            "patient_program_attribute",
            "patient_state",
            "person_address",
            "person_attribute",
            "person_merge_log",
            "person_name",
            "referral_order",
            "relationship",
            "test_order",
            "visit",
            "visit_attribute"
    };

    @Test
    public void shouldGetTablesAndColumns() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
        assertThat(metadata.getDatabaseName(), equalTo("dbevent"));
        assertThat(metadata.getTables().size(), equalTo(185));
        assertThat(metadata.getTable("patient").getColumns().size(), equalTo(10));
        assertTrue(metadata.getColumn("encounter", "encounter_id").isPrimaryKey());
    }

    @Test
    public void shouldGetTablesWithReferencesTo() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
        Set<String> ppRefs = metadata.getTablesWithReferencesTo("patient_program");
        assertThat(ppRefs.size(), equalTo(2));
        assertTrue(ppRefs.contains("patient_state"));
        assertTrue(ppRefs.contains("patient_program_attribute"));
        String[] excludedTables = {"users", "provider"};
        Set<String> personRefs = metadata.getTablesWithReferencesTo("person", excludedTables);
        assertEquals(EXPECTED_TABLES.length, personRefs.size());
        for (String expectedTable : EXPECTED_TABLES) {
            assertTrue(personRefs.contains(expectedTable));
        }
    }

    @Test
    public void shouldGetUniquePathsToColumn() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
        DatabaseColumn toColumn = metadata.getTable("person").getColumn("person_id");
        List<String> exclusions = Arrays.asList("users", "provider");
        Map<String, List<DatabaseJoinPath>> resultsByTable = new LinkedHashMap<>();
        for (String tableName : metadata.getTables().keySet()) {
            DatabaseTable table = metadata.getTable(tableName);
            if (!exclusions.contains(tableName)) {
                List<DatabaseJoinPath> tablePaths = metadata.getPathsToColumn(table, toColumn, exclusions);
                if (!tablePaths.isEmpty()) {
                    Collections.sort(tablePaths, Comparator.comparing(DatabaseJoinPath::toString));
                    resultsByTable.put(tableName, tablePaths);
                    log.debug("********" + tableName + "********");
                    for (DatabaseJoinPath path : tablePaths) {
                        log.debug(path);
                    }
                }
            }
        }
        assertThat(resultsByTable.size(), equalTo(EXPECTED_TABLES.length));
        assertJoinPath(resultsByTable.get("address_hierarchy_address_to_entry_map").get(0), "person_address", "person");
        assertJoinPath(resultsByTable.get("allergy").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("allergy_reaction").get(0), "allergy", "patient", "person");
        assertJoinPath(resultsByTable.get("appointmentscheduling_appointment").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("appointmentscheduling_appointment_request").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("appointmentscheduling_appointment_status_history").get(0), "appointmentscheduling_appointment", "patient", "person");
        assertJoinPath(resultsByTable.get("cohort_member").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("concept_proposal").get(0), "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("concept_proposal").get(1), "obs", "person");
        assertJoinPath(resultsByTable.get("concept_proposal_tag_map").get(0), "concept_proposal", "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("concept_proposal_tag_map").get(1), "concept_proposal", "obs", "person");
        assertJoinPath(resultsByTable.get("conditions").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("diagnosis_attribute").get(0), "encounter_diagnosis", "patient", "person");
        assertJoinPath(resultsByTable.get("drug_order").get(0), "orders", "patient", "person");
        assertJoinPath(resultsByTable.get("emr_radiology_order").get(0), "test_order", "orders", "patient", "person");
        assertJoinPath(resultsByTable.get("encounter").get(0),  "patient", "person");
        assertJoinPath(resultsByTable.get("encounter_diagnosis").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("encounter_provider").get(0), "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("fhir_diagnostic_report").get(0), "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("fhir_diagnostic_report").get(1), "patient", "person");
        assertJoinPath(resultsByTable.get("fhir_diagnostic_report_performers").get(0), "fhir_diagnostic_report", "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("fhir_diagnostic_report_performers").get(1), "fhir_diagnostic_report", "patient", "person");
        assertJoinPath(resultsByTable.get("fhir_diagnostic_report_results").get(0), "obs", "person");
        assertJoinPath(resultsByTable.get("logic_rule_token").get(0), "person");
        assertJoinPath(resultsByTable.get("logic_rule_token_tag").get(0), "logic_rule_token", "person");
        assertJoinPath(resultsByTable.get("name_phonetics").get(0), "person_name", "person");
        assertJoinPath(resultsByTable.get("note").get(0), "encounter", "patient", "person");
        assertJoinPath(resultsByTable.get("note").get(1), "obs", "person");
        assertJoinPath(resultsByTable.get("note").get(2), "patient", "person");
        assertJoinPath(resultsByTable.get("obs").get(0), "person");
        assertJoinPath(resultsByTable.get("order_attribute").get(0), "orders", "patient", "person");
        assertJoinPath(resultsByTable.get("order_group").get(0),  "patient", "person");
        assertJoinPath(resultsByTable.get("order_group_attribute").get(0), "order_group", "patient", "person");
        assertJoinPath(resultsByTable.get("orders").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("paperrecord_paper_record").get(0), "patient_identifier", "patient", "person");
        assertJoinPath(resultsByTable.get("paperrecord_paper_record_merge_request").get(0), "paperrecord_paper_record", "patient_identifier", "patient", "person");
        assertJoinPath(resultsByTable.get("paperrecord_paper_record_merge_request").get(1), "paperrecord_paper_record", "patient_identifier", "patient", "person");
        assertJoinPath(resultsByTable.get("paperrecord_paper_record_request").get(0), "person");
        assertJoinPath(resultsByTable.get("patient").get(0), "person");
        assertJoinPath(resultsByTable.get("patient_identifier").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("patient_program").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("patient_program_attribute").get(0), "patient_program", "patient", "person");
        assertJoinPath(resultsByTable.get("patient_state").get(0), "patient_program", "patient", "person");
        assertJoinPath(resultsByTable.get("person_address").get(0), "person");
        assertJoinPath(resultsByTable.get("person_attribute").get(0), "person");
        assertJoinPath(resultsByTable.get("person_merge_log").get(0), "person");
        assertJoinPath(resultsByTable.get("person_merge_log").get(1), "person");
        assertJoinPath(resultsByTable.get("person_name").get(0), "person");
        assertJoinPath(resultsByTable.get("referral_order").get(0), "orders", "patient", "person");
        assertJoinPath(resultsByTable.get("relationship").get(0), "person");
        assertJoinPath(resultsByTable.get("relationship").get(1), "person");
        assertJoinPath(resultsByTable.get("test_order").get(0), "orders", "patient", "person");
        assertJoinPath(resultsByTable.get("visit").get(0), "patient", "person");
        assertJoinPath(resultsByTable.get("visit_attribute").get(0), "visit", "patient", "person");
    }

    protected void assertJoinPath(DatabaseJoinPath path, String... joinTables) {
        assertThat(path.size(), equalTo(joinTables.length));
        for (int i=0; i<path.size(); i++) {
            assertThat(path.get(i).getPrimaryKey().getTableName(), equalTo(joinTables[i]));
        }
    }

    @Test
    public void shouldGetPatientTableNames() throws Exception {
        EventContext ctx = MysqlExtension.getEventContext();
        DatabaseMetadata metadata = ctx.getDatabase().getMetadata();
        Set<String> tableNames = metadata.getPatientTableNames();
        assertThat(tableNames.size(), equalTo(EXPECTED_TABLES.length + 1)); // Deps + person
        for (String expectedTable : EXPECTED_TABLES) {
            assertTrue(tableNames.contains(expectedTable));
        }
        assertTrue(tableNames.contains("person"));
    }

    public void print(DatabaseMetadata metadata) {
        for (DatabaseTable table : metadata.getTables().values()) {
            System.out.println("=======================");
            System.out.println("TABLE: " + table.getTableName());
            for (DatabaseColumn column : table.getColumns().values()) {
                System.out.println(" " + column.getColumnName() + (column.isPrimaryKey() ? " PRIMARY KEY" : ""));
                for (DatabaseColumn fkColumn : column.getReferences()) {
                    System.out.println("   => " + fkColumn.getTableName() + "." + fkColumn.getColumnName());
                }
                for (DatabaseColumn fkColumn : column.getReferencedBy()) {
                    System.out.println("   <= " + fkColumn.getTableName() + "." + fkColumn.getColumnName());
                }
            }
        }
    }
}
