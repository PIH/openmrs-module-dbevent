package org.openmrs.module.dbevent;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DbEventModuleActivatorTest extends BaseDbEventTest {

	Logger logger = (Logger) LogManager.getLogger(DbEventModuleActivator.class);

	@BeforeEach
	@Override
	public void setup() {
		super.setup();
		addMemoryAppenderToLogger(logger, Level.INFO);
	}

	@AfterEach
	@Override
	public void teardown() {
		super.teardown();
	}

	@Test
	public void shouldLogAtStartupAndShutdown() {
		DbEventModuleActivator activator = new DbEventModuleActivator();
		activator.started();
		assertLastLogContains("DB Event Module Started");
		activator.stopped();
		assertLastLogContains("DB Event Module Stopped");
	}
}
