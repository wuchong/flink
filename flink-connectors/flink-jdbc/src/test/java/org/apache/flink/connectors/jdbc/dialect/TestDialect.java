/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.jdbc.dialect;

import org.apache.flink.connectors.jdbc.JdbcDataType;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

/**
 * A mock {@link JdbcDialect}.
 */
public class TestDialect implements JdbcDialect {
	@Override
	public boolean canHandle(String jdbcUrl) {
		return jdbcUrl.startsWith("jdbc:test:");
	}

	@Override
	public String defaultDriverName() {
		return "testDialect";
	}

	@Override
	public void validateInternalType(LogicalType type) {

	}

	@Override
	public void validateExternalType(JdbcDataType type) {

	}

	@Override
	public LogicalType getInternalType(JdbcDataType externalType) {
		return null;
	}

	@Override
	public JdbcDataType getExternalType(LogicalType internalType) {
		return null;
	}

	@Override
	public JdbcRuntimeConverter getInputConverter(RowType rowType) {
		return null;
	}

	@Override
	public RowToJdbcConverter getOutputConverter(JdbcDataType[] jdbcDataTypes) {
		return null;
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "*" + identifier + "*";
	}

	@Override
	public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		return null;
	}

	@Override
	public String getInsertIntoStatement(String tableName, String[] fieldNames) {
		return null;
	}

	@Override
	public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
		return null;
	}

	@Override
	public String getDeleteStatement(String tableName, String[] conditionFields) {
		return null;
	}

	@Nullable
	@Override
	public String getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		return null;
	}

	@Override
	public String getRowExistsStatement(String tableName, String[] conditionFields) {
		return null;
	}

	@Override
	public String getTableExistsStatement(String tableName) {
		return null;
	}

	@Override
	public String getCreateTableStatement(String tableName, String[] fieldNames, JdbcDataType[] types, String[] uniqueKeyFields) {
		return null;
	}
}
