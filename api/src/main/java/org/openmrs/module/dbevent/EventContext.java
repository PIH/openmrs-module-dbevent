package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.api.context.Context;
import org.openmrs.util.OpenmrsUtil;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Simple wrapper class access that provides access to the OpenMRS Context and related services
 */
@Data
public class EventContext {

    private static final Logger log = LogManager.getLogger(EventContext.class);

    private File applicationDataDir;
    private Properties runtimeProperties;

    public EventContext() {
        applicationDataDir = OpenmrsUtil.getApplicationDataDirectoryAsFile();
        runtimeProperties = Context.getRuntimeProperties();
    }

    public File getModuleDataDir() {
        return new File(applicationDataDir, "dbevent");
    }

    public Connection openConnection() {
        try {
            String url = runtimeProperties.getProperty("connection.url");
            String username = runtimeProperties.getProperty("connection.username");
            String password = runtimeProperties.getProperty("connection.password");
            return DriverManager.getConnection(url, username, password);
        }
        catch (Exception e) {
            throw new RuntimeException("An error occurred opening a database connection", e);
        }
    }

    public void closeConnection(Connection connection) {
        try {
            connection.close();
        }
        catch (Exception e) {
            log.warn("An error occurred closing the database connection", e);
        }
    }
}
