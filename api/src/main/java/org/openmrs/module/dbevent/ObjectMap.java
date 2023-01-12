package org.openmrs.module.dbevent;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.HashMap;

/**
 * Simple HashMap extension that contains utility methods for retrieving / converting values to certain types
 */
public class ObjectMap extends HashMap<String, Object> {

    public ObjectMap() {
        super();
    }

    public ObjectMap(Struct struct) {
        this();
        if (struct != null && struct.schema() != null) {
            for (Field field : struct.schema().fields()) {
                put(field.name(), struct.get(field));
            }
        }
    }

    public Integer getInteger(String key) {
        Object ret = get(key);
        return ret == null ? null : (Integer)ret;
    }

    public String getString(String key) {
        Object ret = get(key);
        return ret == null ? null : ret.toString();
    }

    public Boolean getBoolean(String key) {
        Object ret = get(key);
        if (ret == null) { return null; }
        if (ret instanceof Boolean) { return (Boolean)ret; }
        if (ret instanceof Number) { return ((Number)ret).intValue() == 1; }
        return Boolean.parseBoolean(ret.toString());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        Boolean b = getBoolean(key);
        return b == null ? defaultValue : b;
    }
}