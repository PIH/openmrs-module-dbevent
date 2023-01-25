package org.openmrs.module.dbevent;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;

/**
 * Simple HashMap extension that contains utility methods for retrieving / converting values to certain types
 */
public class ObjectMap extends HashMap<String, Object> {

    /**
     * Default constructor
     */
    public ObjectMap() {
        super();
    }

    /**
     * Constructs a new ObjectMap from the given Struct, using the fields of the schema as keys
     * @param struct the struct to convert to an ObjectMap
     */
    public ObjectMap(Struct struct) {
        this();
        if (struct != null && struct.schema() != null) {
            for (Field field : struct.schema().fields()) {
                put(field.name(), struct.get(field));
            }
        }
    }

    /**
     * @return the value with the given key cast as an Integer
     */
    public Integer getInteger(String key) {
        return (Integer) get(key);
    }

    /**
     * @return the value with the given key cast as a Long
     */
    public Long getLong(String key) {
        return (Long) get(key);
    }

    /**
     * @return the toString representation of the value with the given key, or null if not found
     */
    public String getString(String key) {
        Object ret = get(key);
        return ret == null ? null : ret.toString();
    }

    /**
     * @return the value with the given key as a cast or parsed boolean value
     */
    public Boolean getBoolean(String key) {
        Object ret = get(key);
        if (ret == null) { return null; }
        if (ret instanceof Boolean) { return (Boolean)ret; }
        if (ret instanceof Number) { return ((Number)ret).intValue() == 1; }
        return Boolean.parseBoolean(ret.toString());
    }

    /**
     * @return the value with the given key as a cast or parsed boolean value, or the defaultValue if null
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Boolean b = getBoolean(key);
        return b == null ? defaultValue : b;
    }
}