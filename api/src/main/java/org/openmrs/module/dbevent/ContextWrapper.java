package org.openmrs.module.dbevent;

import lombok.Data;
import org.openmrs.api.context.Context;
import org.openmrs.util.OpenmrsUtil;

import java.io.File;
import java.util.Properties;

/**
 * Simple wrapper class access that provides access to the OpenMRS Context and related services
 */
@Data
public class ContextWrapper {

    private File applicationDataDir;
    private Properties runtimeProperties;

    public ContextWrapper() {
        applicationDataDir = OpenmrsUtil.getApplicationDataDirectoryAsFile();
        runtimeProperties = Context.getRuntimeProperties();
    }
}
