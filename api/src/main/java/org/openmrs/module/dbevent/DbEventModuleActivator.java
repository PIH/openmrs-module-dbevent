/*
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.dbevent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.BaseModuleActivator;
import org.openmrs.module.dbevent.consumer.LoggingEventConsumer;

import java.util.Collections;

/**
 * This class contains the logic that is run every time this module is either started or shutdown
 */
public class DbEventModuleActivator extends BaseModuleActivator {

	private static final Logger log = LogManager.getLogger(DbEventModuleActivator.class);
	
	@Override
	public void started() {
		log.info("DB Event Module Started");

		// TODO: Temporary for testing.  This would be added by a downstream module
		DbEventSource eventSource = new DbEventSource(100002,"EventLogger", new ContextWrapper());
		eventSource.setEventConsumer(new LoggingEventConsumer());
		eventSource.configureTablesToInclude(Collections.singletonList("*"));
		eventSource.start();
	}
	
	@Override
	public void stopped() {
		log.info("DB Event Module Stopped");
	}
}
