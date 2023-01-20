package org.openmrs.module.dbevent.patient;

import lombok.Data;
import org.openmrs.module.dbevent.DbEvent;

/**
 * Specifies a DbEvent related to a specific patient
 */
@Data
public class PatientEvent {

    private Integer patientId;
    private DbEvent dbEvent;

    public PatientEvent(Integer patientId, DbEvent dbEvent) {
        this.patientId = patientId;
        this.dbEvent = dbEvent;
    }
}
