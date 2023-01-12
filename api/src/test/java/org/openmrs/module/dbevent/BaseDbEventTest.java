package org.openmrs.module.dbevent;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.openmrs.api.context.Context;
import org.openmrs.logging.MemoryAppender;
import org.openmrs.util.OpenmrsConstants;
import org.openmrs.util.OpenmrsUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Base class for non-context-sensitive tests for this module
 * Sets up loggers to enable testing of logging events
 */
public abstract class BaseDbEventTest {

	protected MemoryAppender memoryAppender;
	protected File appDataDir;
	protected File runtimePropertiesFile;
	protected List<Logger> loggers;

	@BeforeEach
	public void setup() {
		appDataDir = createAppDataDir();
		runtimePropertiesFile = new File(appDataDir, "openmrs-runtime.properties");
		runtimePropertiesFile.deleteOnExit();
		PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
		memoryAppender = MemoryAppender.newBuilder().setLayout(layout).build();
		memoryAppender.start();
		Properties p = new Properties();
		p.put("connection.username", "admin");
		p.put("connection.password", "Admin123");
		p.put("connection.url", "jdbc:mysql://localhost:3306/openmrs");
		setRuntimeProperties(p);
		loggers = new ArrayList<>();
	}

	protected void addMemoryAppenderToLogger(Logger logger, Level level) {
		logger.setAdditive(false);
		logger.setLevel(level);
		logger.addAppender(memoryAppender);
		loggers.add(logger);
	}

	protected File createAppDataDir() {
		try {
			File appDataDir = File.createTempFile(UUID.randomUUID().toString(), "");
			appDataDir.delete();
			appDataDir.mkdir();
			appDataDir.deleteOnExit();
			return appDataDir;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected void setRuntimeProperties(Properties p) {
		if (runtimePropertiesFile != null && runtimePropertiesFile.exists()) {
			runtimePropertiesFile.delete();
		}
		p.setProperty(OpenmrsConstants.APPLICATION_DATA_DIRECTORY_RUNTIME_PROPERTY, appDataDir.getAbsolutePath());
		OpenmrsUtil.storeProperties(p, runtimePropertiesFile, "test");
		Context.setRuntimeProperties(p);
	}

	@AfterEach
	public void teardown() {
		for (Logger logger : loggers) {
			logger.removeAppender(memoryAppender);
		}
		memoryAppender.stop();
		((Logger) LogManager.getRootLogger()).getContext().updateLoggers();
		memoryAppender = null;
		loggers = null;
		if (runtimePropertiesFile != null && runtimePropertiesFile.exists()) {
			runtimePropertiesFile.delete();
		}
		if (appDataDir != null && appDataDir.exists()) {
			appDataDir.delete();
		}
	}

	/**
	 * @param test if the last line logged into the memory appender contains the test string, pass.  Otherwise fail
	 */
	protected void assertLastLogContains(String test) {
		assertThat(memoryAppender.getLogLines(), notNullValue());
		int numLines = memoryAppender.getLogLines().size();
		assertThat(numLines, greaterThan(0));
		String line = memoryAppender.getLogLines().get(numLines-1);
		assertThat(line, containsString(test));
	}
}
