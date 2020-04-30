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
import org.apache.flink.connectors.jdbc.JdbcDataType;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.util.Optional;


/**
 * This interface is responsible for defining a JDBC dialect which used to interact with Flink SQL system.
 */
@PublicEvolving
public interface JdbcDialect extends Serializable {

	/**
	 *
	 * @return
	 */
	String dialectName();

	String defaultDriverName();

	/**
	 * validate supported internalType before write data to db.
	 * @param type
	 */
	void supportsLogicalType(LogicalType type);

	JdbcRuntimeConverter getInputConverter(RowType rowType);

	String quoteIdentifier(String identifier);

	String getSelectStatement(String tableName, String[] selectFields, String[] conditionFields);

	String getInsertStatement(String tableName, String[] fieldNames);

	String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields);

	String getDeleteStatement(String tableName, String[] conditionFields);

	Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields);

	String getRowExistsStatement(String tableName, String[] conditionFields);

//	Optional<LogicalType> getFlinkLogicalType(int jdbcType, String typeName, int precision, int scale);
//
//	JdbcDataType getJdbcDataType(LogicalType flinkType);
//
//	String getTableExistsStatement(String tableName);
//
//	String getCreateTableStatement(String tableName, TableSchema schema);
}

