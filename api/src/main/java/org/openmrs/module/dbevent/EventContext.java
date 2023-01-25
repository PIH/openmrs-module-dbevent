package org.openmrs.module.dbevent;

import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.api.context.Context;
import org.openmrs.util.OpenmrsUtil;

import java.io.File;
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
