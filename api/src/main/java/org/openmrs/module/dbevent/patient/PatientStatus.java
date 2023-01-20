package org.openmrs.module.dbevent.patient;

import lombok.Data;

import java.io.Serializable;

/**
 * Represents the update status of a particular patient
 */
@Data
public class PatientStatus implements Serializable {
    private Integer patientId;
    private Long lastUpdated;
    private boolean deleted = false;

    /**
     * Updates the lastUpdated date with the timestamp if the timestamp is not null, and if the
     * timestamp is later than th current value or if the current value is null
     * @param timestamp the timestamp to update
     */
    public void updateLastUpdatedIfLater(Long timestamp) {
        if (timestamp != null) {
            if (lastUpdated == null || lastUpdated < timestamp) {
                lastUpdated = timestamp;
            }
        }
    }
}
