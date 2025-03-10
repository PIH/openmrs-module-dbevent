/*
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.dbevent.monitoring;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openmrs.module.dbevent.DbEventListener;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class supports JMX integration by exposing and retrieving information from JMX
 */
public class DbEventMonitor {

	private static final Logger log = LogManager.getLogger(DbEventMonitor.class);
	private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

	public static void registerMonitoringBean(DbEventListener listener) {
		try {
			String name = getDbEventStatusMBeanName(listener);
			mbeanServer.registerMBean(listener.getStatus(), new ObjectName(name));
		}
		catch (Exception e) {
			log.warn("Error registering monitoring bean for {}", listener.getName(), e);
		}
	}

	public static void unregisterMonitoringBean(DbEventListener listener) {
		try {
			String name = getDbEventStatusMBeanName(listener);
			ObjectName mbeanName = new ObjectName(name);
			if (mbeanServer.isRegistered(mbeanName)) {
				mbeanServer.unregisterMBean(new ObjectName(name));
			}
			else {
				log.warn("Unable to unregister monitoring bean {} as it is not registered", name);
			}
		}
		catch (Exception e) {
			log.warn("Error unregistering monitoring bean for {}", listener.getName(), e);
		}
	}

	/**
	 * @return the value of the all status attributes for the given listener
	 */
	public static Map<String, Object> getDbEventListenerStatusAttributes(DbEventListener listener) {
		String name = getDbEventStatusMBeanName(listener);
		return getMBeanAttributes(name);
	}

	public static DbEventListenerStatus getDbEventListenerStatus(DbEventListener listener) {
		Map<String, Object> m = getDbEventListenerStatusAttributes(listener);
		DbEventListenerStatus status = null;
		if (!m.isEmpty()) {
			status = new DbEventListenerStatus();
			status.setId(listener.getId());
			status.setName(listener.getName());
			status.setStatus((String) m.get("Status"));
			status.setLatestEvent((String) m.get("LatestEvent"));
			status.setLatestEventProcessed((Boolean) m.get("LatestEventProcessed"));
			status.setLatestEventErrorMessage((String) m.get("LatestEventErrorMessage"));
			status.setLatestEventErrorRetryNum((Long) m.get("LatestEventErrorRetryNum"));
			TabularData td = (TabularData) m.get("EventsProcessedByTable");
			for (Object row : td.values()) {
				CompositeData cd = (CompositeData) row;
				List<?> values = new ArrayList<>(cd.values());
				status.getEventsProcessedByTable().put((String) values.get(0), (Long) values.get(1));
			}
		}
		return status;
	}

	/**
	 * Get the name of the DbEventListenerStatus MBean for the given listener
	 */
	public static String getDbEventStatusMBeanName(DbEventListener listener) {
		String packageName = DbEventListenerStatus.class.getPackage().getName();
		String className = DbEventListenerStatus.class.getSimpleName();
		return packageName + ":type=" + className + ",id=" + listener.getId();
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
			log.trace("An error occurred trying to get monitoring attributes for {}", name, e);
		}
		return ret;
	}
}
