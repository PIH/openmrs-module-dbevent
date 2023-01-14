package org.openmrs.module.dbevent.test;

import org.apache.kafka.common.Uuid;
import org.openmrs.module.dbevent.ContextWrapper;

import java.io.File;

public class TestContextWrapper extends ContextWrapper {

    public TestContextWrapper(Mysql mysql) {
        setRuntimeProperties(mysql.getConnectionProperties());
        setApplicationDataDir( new File(System.getProperty("java.io.tmpdir"), Uuid.randomUuid().toString()));
    }
}
