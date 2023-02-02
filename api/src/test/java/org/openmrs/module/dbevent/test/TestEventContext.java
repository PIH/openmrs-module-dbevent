package org.openmrs.module.dbevent.test;

import org.apache.kafka.common.Uuid;
import org.openmrs.module.dbevent.EventContext;

import java.io.File;
import java.util.Properties;

public class TestEventContext extends EventContext {

    public TestEventContext(Properties connectionProperties) {
        setRuntimeProperties(connectionProperties);
        setApplicationDataDir( new File(System.getProperty("java.io.tmpdir"), Uuid.randomUuid().toString()));
    }
}
