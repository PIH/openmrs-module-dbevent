package org.openmrs.module.dbevent.test;

import org.apache.commons.lang.StringUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

public class MysqlExtension implements BeforeAllCallback, ExtensionContext.Store.CloseableResource {

    private static final Logger log = LoggerFactory.getLogger(MysqlExtension.class);

    private static Properties mysqlProperties = null;
    private static Mysql mysql = null;

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws Exception {
        String uniqueKey = this.getClass().getName();
        ExtensionContext.Store globalStore = extensionContext.getRoot().getStore(GLOBAL);
        Object contextVal = globalStore.get(uniqueKey);
        if (contextVal == null) {
            globalStore.put(uniqueKey, this);
            mysqlProperties = loadExternalMysqlProperties();
            if (mysqlProperties.isEmpty()) {
                log.warn("Opening MySQL database");
                mysql = Mysql.open();
                mysqlProperties = mysql.getConnectionProperties();
            }
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
        return new TestEventContext(mysqlProperties);
    }

    protected Properties loadExternalMysqlProperties() throws Exception {
        Properties p = new Properties();
        String propertiesFile = System.getProperty("MYSQL_PROPERTIES_FILE");
        if (StringUtils.isNotBlank(propertiesFile)) {
            try (InputStream is = Files.newInputStream(Paths.get(propertiesFile))) {
                p.load(is);
            }
        }
        return p;
    }
}
