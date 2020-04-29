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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcToRowConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.Serializable;


/**
 * This interface is responsible for defining a JDBC dialect which used to interact with Flink SQL system.
 */
@PublicEvolving
public interface JdbcDialect extends Serializable {

	boolean canHandle(String jdbcUrl);

	String defaultDriverName();

	/**
	 * validate supported internalType before write data to db.
	 * @param type
	 */
	void validateInternalType(LogicalType type);

	/**
	 * validate supported external before write data to db.
	 * @param type
	 */
	void validateExternalType(JdbcType type);

	LogicalType getInternalType(JdbcType externalType);

	JdbcType getExternalType(LogicalType internalType);

	JdbcToRowConverter getInputConverter(RowType rowType);

	RowToJdbcConverter getOutputConverter(JdbcType[] jdbcTypes);

	String quoteIdentifier(String identifier);

	String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields);

	String getInsertIntoStatement(String tableName, String[] fieldNames);

	String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields);

	String getDeleteStatement(String tableName, String[] conditionFields);

	@Nullable
	String getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields);

	String getRowExistsStatement(String tableName, String[] conditionFields);

	String getTableExistsStatement(String tableName);

	String getCreateTableStatement(String tableName, String[] fieldNames, JdbcType[] types, String[] uniqueKeyFields);
}

