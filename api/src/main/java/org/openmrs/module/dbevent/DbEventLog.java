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

import java.util.HashMap;
import java.util.Map;

/**
 * This class provides access to statistics and current status of any executing DbEventSources
 */
public class DbEventLog {

	private static final Logger log = LogManager.getLogger(DbEventLog.class);
	private static final Map<String, DbEventStatus> latestEvents = new HashMap<>();
	private static final Map<String, Map<String, Integer>> tableCounts = new HashMap<>();

	public static synchronized DbEventStatus log(DbEvent event) {
		// Log the event, if enabled
		if (log.isTraceEnabled()) {
			log.trace(event);
		}

		// Track the total number of table rows processed for a given source
		getTableCounts(event.getServerName()).merge(event.getTable(), 1, Integer::sum);

		// Construct an Event status and set it as the most recent for the associated source and return it
		DbEventStatus status = new DbEventStatus(event);
		latestEvents.put(event.getServerName(), status);
		return status;
	}

	public static DbEventStatus getLatestEventStatus(String source) {
		return latestEvents.get(source);
	}

	public Map<String, DbEventStatus> getLatestEventStatuses() {
		return latestEvents;
	}

	public static Map<String, Map<String, Integer>> getTableCounts() {
		return tableCounts;
	}

	public static Map<String, Integer> getTableCounts(String source) {
		return tableCounts.computeIfAbsent(source, k -> new HashMap<>());
	}
}
