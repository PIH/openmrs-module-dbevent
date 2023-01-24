package org.openmrs.module.dbevent;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

/**
 * Represents metadata for a Database
 */
@Data
@AllArgsConstructor
public class DatabaseJoin implements Serializable {
    private DatabaseColumn fromColumn;
    private DatabaseColumn toColumn;

    @Override
    public String toString() {
        return fromColumn + " -> " + toColumn;
    }
}