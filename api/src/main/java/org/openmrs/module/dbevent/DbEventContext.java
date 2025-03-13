package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.api.context.Context;
import org.openmrs.module.dbevent.database.Database;
import org.openmrs.util.OpenmrsUtil;

import java.io.File;
import java.util.Properties;

/**
 * This class is a simple wrapper around aspects of OpenMRS, primarily so that these can be mocked for testing
 */
@Data
public class DbEventContext {

    private static final Logger log = LogManager.getLogger(DbEventContext.class);

    private File applicationDataDir;
    private Properties runtimeProperties;

    public DbEventContext() {
        applicationDataDir = OpenmrsUtil.getApplicationDataDirectoryAsFile();
        runtimeProperties = Context.getRuntimeProperties();
    }

    /**
     * @return a database object constructed from the given runtime properties.
     */
    public Database getDatabase() {
        return new Database(runtimeProperties);
    }

    /**
     * @return the directory for module-related data
     */
    public File getModuleDataDir() {
        return new File(applicationDataDir, "dbevent");
    }
}
