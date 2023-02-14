package org.openmrs.module.dbevent;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Represents a database query with positional parameters
 * and a List of Strings that represent the keys from a Map for retrieving the values of the positional parameters
 * This is intended such that if you had:
 * sql = "select patient_id from encounter where encounter_id = ?"
 * parameterNames = ["encounter_id"]
 * This would enable one to execute this query, and retrieve the appropriate value of the positional parameter
 * from another Map with {"encounter_id": 1234}
 */
@Data
@AllArgsConstructor
public class DatabaseQuery implements Serializable {

    private String sql;
    private List<String> parameterNames;

    @Override
    public String toString() {
        return sql + " " + parameterNames;
    }
}