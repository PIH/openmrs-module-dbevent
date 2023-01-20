package org.openmrs.module.dbevent.test;

import org.apache.kafka.common.Uuid;
import org.openmrs.module.dbevent.EventContext;

import java.io.File;

public class TestEventContext extends EventContext {

    public TestEventContext(Mysql mysql) {
        setRuntimeProperties(mysql.getConnectionProperties());
        setApplicationDataDir( new File(System.getProperty("java.io.tmpdir"), Uuid.randomUuid().toString()));
    }
}
