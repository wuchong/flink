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

package org.apache.flink.connectors.jdbc.source.row.converter;

import org.apache.flink.connectors.jdbc.JdbcType;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Converter from flink Row to JDBC object.
 */
public interface RowToJdbcConverter extends Serializable {
	/**
	 * Convert data retrieved from  internal Row to JDBC Object.
	 *
	 * @param internalData The row to set
	 */
	PreparedStatement toExternal(Row internalData, PreparedStatement statement) throws SQLException;

	/**
	 * Runtime converter to convert JDBC field to Java objects.
	 */
	@FunctionalInterface
	interface RowFieldConverter extends Serializable {
		void convert(PreparedStatement statement, int index, JdbcType jdbcType, Object value) throws SQLException;
	}
}
