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
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * This class supports JMX integration by exposing and retrieving information from JMX
 */
public class DbEventMonitor {

	private static final Logger log = LogManager.getLogger(DbEventMonitor.class);
	private static final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

	public static void registerMonitoringBeans(DbEventListener listener) {
		try {
			ObjectName name = getDbEventStatusMBeanName(listener);
			mbeanServer.registerMBean(listener.getStatus(), name);
		}
		catch (Exception e) {
			log.warn("Error registering monitoring bean for {}", listener.getConfig().getSourceName(), e);
		}
	}

	public static void unregisterMonitoringBeans(DbEventListener listener) {
		try {
			ObjectName name = getDbEventStatusMBeanName(listener);
			if (mbeanServer.isRegistered(name)) {
				mbeanServer.unregisterMBean(name);
			}
		}
		catch (Exception e) {
			log.warn("Error unregistering monitoring bean for {}", listener.getConfig().getSourceName(), e);
		}
	}

	/**
	 * @return the value of the all status attributes for the given listener
	 */
	public static Map<String, Object> getDbEventListenerStatusAttributes(DbEventListener listener) {
		ObjectName name = getDbEventStatusMBeanName(listener);
		return getMBeanAttributes(name);
	}

	/**
	 * Get the status of the given DbEventListener
	 */
	public static DbEventListenerStatus getDbEventListenerStatus(DbEventListener listener) {
		Map<String, Object> m = getDbEventListenerStatusAttributes(listener);
		DbEventListenerStatus status = null;
		if (!m.isEmpty()) {
			status = new DbEventListenerStatus();
			status.setId(listener.getConfig().getSourceId());
			status.setName(listener.getConfig().getSourceName());
			status.setStatus((String) m.get("Status"));
			status.setLatestEvent((String) m.get("LatestEvent"));
			status.setLatestEventProcessed((Boolean) m.get("LatestEventProcessed"));
			status.setLatestEventErrorMessage((String) m.get("LatestEventErrorMessage"));
			status.setLatestEventErrorRetryNum((Long) m.get("LatestEventErrorRetryNum"));
			status.setNumberOfReads((Long) m.get("NumberOfReads"));
			status.setNumberOfInserts((Long) m.get("NumberOfInserts"));
			status.setNumberOfUpdates((Long) m.get("NumberOfUpdates"));
			status.setNumberOfDeletes((Long) m.get("NumberOfDeletes"));
		}
		return status;
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
			ret = getMBeanAttributes(new ObjectName(name));
		}
		catch (Exception e) {
			log.trace("An error occurred trying to get monitoring attributes for {}", name, e);
		}
		return ret;
	}

	private static Map<String, Object> getMBeanAttributes(ObjectName name) {
		Map<String, Object> ret = new HashMap<>();
		try {
			MBeanInfo beanInfo = mbeanServer.getMBeanInfo(name);
			for (MBeanAttributeInfo attribute : beanInfo.getAttributes()) {
				String attributeName = attribute.getName();
				ret.put(attributeName, mbeanServer.getAttribute(name, attributeName));
			}
		}
		catch (Exception e) {
			log.trace("An error occurred trying to get monitoring attributes for {}", name, e);
		}
		return ret;
	}

	/**
	 * Get the name of the DbEventListenerStatus MBean for the given listener
	 */
	public static ObjectName getDbEventStatusMBeanName(DbEventListener listener) {
		try {
			String packageName = DbEventListenerStatus.class.getPackage().getName();
			String className = DbEventListenerStatus.class.getSimpleName();
			return new ObjectName(packageName + ":type=" + className + ",id=" + listener.getConfig().getSourceId());
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
