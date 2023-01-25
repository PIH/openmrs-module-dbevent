package org.openmrs.module.dbevent.test;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class MysqlExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static final Logger log = LoggerFactory.getLogger(MysqlExtension.class);

    private static Mysql mysql;

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        String uniqueKey = this.getClass().getName();
        ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
        Object contextVal = globalStore.get(uniqueKey);
        if (contextVal == null) {
            globalStore.put(uniqueKey, this);
            log.warn("Opening MySQL database");
            mysql = Mysql.open();
        }
    }

    @Override
    public void close() {
        if (mysql != null) {
            log.warn("Closing MySQL database");
            mysql.close();
        }
    }

    public static TestEventContext getEventContext() {
        return new TestEventContext(mysql);
    }
}
