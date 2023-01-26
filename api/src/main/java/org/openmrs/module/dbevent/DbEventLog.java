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

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * This class provides access to statistics and current status of any executing DbEventSources
 */
public class DbEventLog {

	private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
	private static final Logger log = LogManager.getLogger(DbEventLog.class);
	private static final Map<String, DbEventStatus> latestEvents = new HashMap<>();
	private static final Map<String, Map<String, Integer>> tableCounts = new HashMap<>();

	/**
	 * Provides a mechanism to log each event, in order to
	 * have access to basic counts of progress for each source and information on the latest event sent
	 * @param event the event to log
	 * @return a new DbEventStatus for the given event
	 */
	public static synchronized DbEventStatus log(DbEvent event) {
		// Log the event, if enabled
		if (log.isTraceEnabled()) {
			log.trace(event);
		}

		// Track the total number of table rows processed for a given source
		getTableCounts(event.getSourceName()).merge(event.getTable(), 1, Integer::sum);

		// Construct an Event status and set it as the most recent for the associated source and return it
		DbEventStatus status = new DbEventStatus(event);
		latestEvents.put(event.getSourceName(), status);
		return status;
	}

	/**
	 * @param source the source to query
	 * @return the DbEventStatus representing the status of the most recently logged DbEvent
	 */
	public static DbEventStatus getLatestEventStatus(String source) {
		return latestEvents.get(source);
	}

	/**
	 * @return the DbEventStatus representing the status of the most recently logged DbEvent for each source
	 */
	public Map<String, DbEventStatus> getLatestEventStatuses() {
		return latestEvents;
	}

	/**
	 * @return the running count of events processed by source and table, since the server has started
	 */
	public static Map<String, Map<String, Integer>> getTableCounts() {
		return tableCounts;
	}

	/**
	 * @param source the source to query
	 * @return the running count of events processed by table for the given source, since the server has started
	 */
	public static Map<String, Integer> getTableCounts(String source) {
		return tableCounts.computeIfAbsent(source, k -> new HashMap<>());
	}

	/**
	 * @param sourceName the sourceName to query
	 * @return the value of the all debezium snapshot monitoring bean attributes
	 */
	public static Map<String, Object> getSnapshotMonitoringAttributes(String sourceName) {
		String name = "debezium.mysql:type=connector-metrics,context=snapshot,server=" + sourceName;
		return getMBeanAttributes(name);
	}

	/**
	 * @param sourceName the sourceName to query
	 * @return the value of the all debezium streaming monitoring bean attributes
	 */
	public static Map<String, Object> getStreamingMonitoringAttributes(String sourceName) {
		String name = "debezium.mysql:type=connector-metrics,context=streaming,server=" + sourceName;
		return getMBeanAttributes(name);
	}

	/**
	 * @param name the mbean name to query
	 * @return the value of the all monitoring bean attributes with the given name
	 */
	private static Map<String, Object> getMBeanAttributes(String name) {
		Map<String, Object> ret = new HashMap<>();
		try {
			ObjectName n = new ObjectName(name);
			MBeanInfo beanInfo = mbeanServer.getMBeanInfo(n);
			for (MBeanAttributeInfo attribute : beanInfo.getAttributes()) {
				String attributeName = attribute.getName();
				ret.put(attributeName, mbeanServer.getAttribute(n, attributeName));
			}
		}
		catch (Exception e) {
			log.trace("An error occurred trying to get monitoring attributes for " + name, e);
		}
		return ret;
	}
}
