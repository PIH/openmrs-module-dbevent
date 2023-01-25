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

import lombok.Data;

/**
 * This class represents the status of a particular event
 * This allows tracking events as they are streamed, and provides information on any errors associated
 */
@Data
public class DbEventStatus {

	private final DbEvent event;
	private boolean processed = false;
	private Throwable error;
	private long timestamp = System.currentTimeMillis();

	public DbEventStatus(DbEvent event) {
		this.event = event;
	}
}
