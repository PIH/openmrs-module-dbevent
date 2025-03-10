package org.openmrs.module.dbevent.test;

import org.apache.kafka.common.Uuid;
import org.openmrs.module.dbevent.DbEventContext;

import java.io.File;
import java.util.Properties;

public class TestEventContext extends DbEventContext {

    public TestEventContext(Properties connectionProperties) {
        setRuntimeProperties(connectionProperties);
        setApplicationDataDir( new File(System.getProperty("java.io.tmpdir"), Uuid.randomUuid().toString()));
    }
}
