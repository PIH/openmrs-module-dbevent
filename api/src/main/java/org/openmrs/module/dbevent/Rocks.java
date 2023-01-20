
package org.openmrs.module.dbevent;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;

/**
 * Uses a Rocks DB as a key/value store.
 */
public class Rocks {

    private static final Logger log = LogManager.getLogger(Rocks.class);

    private final RocksDB db;

    public Rocks(File dbFile) {
        RocksDB.loadLibrary();
        final Options options = new Options();
        options.setCreateIfMissing(true);
        try {
            Files.createDirectories(dbFile.getParentFile().toPath());
            db = RocksDB.open(options, dbFile.getAbsolutePath());
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to instantiate rocksdb", e);
        }
    }

    public void put(Serializable key, Serializable value) {
        try {
            db.put(SerializationUtils.serialize(key), SerializationUtils.serialize(value));
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to put into rocks db", e);
        }
    }

    public <T extends Serializable> T get(Serializable key) {
        try {
            if (key != null) {
                byte[] bytes = db.get(SerializationUtils.serialize(key));
                if (bytes != null) {
                    return SerializationUtils.deserialize(bytes);
                }
            }
            return null;
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to get from rocks db", e);
        }
    }

    public <T extends Serializable> T getOrDefault(Serializable key, T defaultValue) {
        T value = get(key);
        return value == null ? defaultValue : value;
    }

    public void delete(Serializable key) {
        try {
            db.delete(SerializationUtils.serialize(key));
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to delete from rocks db", e);
        }
    }

    public void close() {
        try {
            db.close();
        }
        catch (Exception e) {
            log.error("An error occurred while trying to close RocksDB instance", e);
        }
    }
}