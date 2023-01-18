package org.openmrs.module.dbevent;

/**
 * Represents a type of database operation
 */
public enum Operation {

    READ,
    INSERT,
    UPDATE,
    DELETE;

    public static Operation parse(String operation) {
        switch (operation) {
            case "r":
                return READ;
            case "c":
                return INSERT;
            case "u":
                return UPDATE;
            case "d":
                return DELETE;
        }
        throw new IllegalArgumentException("Unknown operation: '" + operation);
    }
}