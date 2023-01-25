package org.openmrs.module.dbevent;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.openmrs.module.dbevent.test.MysqlExtension;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MysqlExtension.class)
public class DatabaseMetadataTest {

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
