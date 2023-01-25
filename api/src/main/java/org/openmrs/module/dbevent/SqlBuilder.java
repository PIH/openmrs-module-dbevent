/**
 * This Source Code Form is subject to the terms of the Mozilla Public License,
 * v. 2.0. If a copy of the MPL was not distributed with this file, You can
 * obtain one at http://mozilla.org/MPL/2.0/. OpenMRS is also distributed under
 * the terms of the Healthcare Disclaimer located at http://openmrs.org/license.
 *
 * Copyright (C) OpenMRS Inc. OpenMRS is a registered trademark and the OpenMRS
 * graphic logo is a trademark of OpenMRS Inc.
 */
package org.openmrs.module.dbevent;

/**
 * Helper class for building and executing an HQL query with parameters
 */
public class SqlBuilder {

	private final StringBuilder sb = new StringBuilder();

	public SqlBuilder select(String... columnNames) {
		for (int i=0; i<columnNames.length; i++) {
			sb.append(i == 0 ? "select " : ", ").append(columnNames[i]).append(" ");
		}
		return this;
	}

	public SqlBuilder from(String tableName) {
		return from(tableName, tableName);
	}

	public SqlBuilder from(String tableName, String alias) {
		sb.append("from ").append(tableName).append(" ").append(alias).append(" ");
		return this;
	}

	public SqlBuilder innerJoin(String fromTable, String fromAlias, String fromColumn, String toTable, String toColumn) {
		sb.append("inner join ").append(fromTable).append(" ").append(fromAlias);
		sb.append(" on ").append(fromAlias).append(".").append(fromColumn);
		sb.append(" = ").append(toTable).append(".").append(toColumn).append(" ");
		return this;
	}

	public SqlBuilder where(String constraint) {
		sb.append(sb.indexOf("where") == -1 ? "where " : "and ").append(constraint).append(" ");
		return this;
	}

	public SqlBuilder append(String clause) {
		sb.append(clause).append(" ");
		return this;
	}

	@Override
	public String toString() {
		return sb.toString();
	}
}
