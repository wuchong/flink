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

import org.apache.flink.connectors.jdbc.JdbcDataType;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Base Converter from flink Row to JDBC object.
 */
public abstract class AbstractRowToJdbcConverter implements RowToJdbcConverter {

	protected final JdbcDataType[] externalTypes;
	protected final RowFieldConverter[] toExternalConverters;

	public AbstractRowToJdbcConverter(JdbcDataType[] externalTypes) {
		if (externalTypes != null && externalTypes.length > 0) {
			this.externalTypes = externalTypes;
			this.toExternalConverters = new RowToJdbcConverter.RowFieldConverter[externalTypes.length];
			for (int i = 0; i < toExternalConverters.length; i++) {
				if (externalTypes == null) {
					toExternalConverters[i] = (statement, index, type, val) ->
						statement.setNull(index + 1, type.getGenericSqlType());
				} else {
					toExternalConverters[i] = createExternalConverter(externalTypes[i].getGenericSqlType());
				}
			}
		} else {
			this.externalTypes = null;
			this.toExternalConverters = null;
		}
	}

	@Override
	public PreparedStatement toExternal(Row internalData, PreparedStatement statement) throws SQLException {
		if (externalTypes == null) {
			for (int index = 0; index < internalData.getArity(); index++) {
				statement.setObject(index + 1, internalData.getField(index));
			}
		} else {
			for (int index = 0; index < internalData.getArity(); index++) {
				toExternalConverters[index].convert(statement, index, externalTypes[index], internalData.getField(index));
			}
		}
		return statement;
	}

	/**
	 * Create a runtime JDBC field converter from given {@link JdbcDataType}.
	 */
	public RowFieldConverter createExternalConverter(int sqlType) {
		return (statement, index, type, val) -> {
			if (val == null) {
				statement.setNull(index + 1, type.getGenericSqlType());
			} else {
				try {
					switch (sqlType) {
						case java.sql.Types.NULL:
							statement.setNull(index + 1, type.getGenericSqlType());
							break;
						case java.sql.Types.BOOLEAN:
						case java.sql.Types.BIT:
							statement.setBoolean(index + 1, (boolean) val);
							break;
						case java.sql.Types.CHAR:
						case java.sql.Types.NCHAR:
						case java.sql.Types.VARCHAR:
						case java.sql.Types.LONGVARCHAR:
						case java.sql.Types.LONGNVARCHAR:
							statement.setString(index + 1, (String) val);
							break;
						case java.sql.Types.TINYINT:
							statement.setByte(index + 1, (byte) val);
							break;
						case java.sql.Types.SMALLINT:
							statement.setShort(index + 1, (short) val);
							break;
						case java.sql.Types.INTEGER:
							statement.setInt(index + 1, (int) val);
							break;
						case java.sql.Types.BIGINT:
							statement.setLong(index + 1, (long) val);
							break;
						case java.sql.Types.REAL:
							statement.setFloat(index + 1, (float) val);
							break;
						case java.sql.Types.FLOAT:
						case java.sql.Types.DOUBLE:
							statement.setDouble(index + 1, (double) val);
							break;
						case java.sql.Types.DECIMAL:
						case java.sql.Types.NUMERIC:
							statement.setBigDecimal(index + 1, (java.math.BigDecimal) val);
							break;
						case java.sql.Types.DATE:
							statement.setDate(index + 1, (java.sql.Date) val);
							break;
						case java.sql.Types.TIME:
							statement.setTime(index + 1, (java.sql.Time) val);
							break;
						case java.sql.Types.TIMESTAMP:
							statement.setTimestamp(index + 1, (java.sql.Timestamp) val);
							break;
						case java.sql.Types.BINARY:
						case java.sql.Types.VARBINARY:
						case java.sql.Types.LONGVARBINARY:
							statement.setBytes(index + 1, (byte[]) val);
							break;
						default:
							statement.setObject(index + 1, val);
					}
				} catch (ClassCastException e) {
					// enrich the exception with detailed information.
					String errorMessage = String.format(
						"%s, field index: %s, field value: %s.", e.getMessage(), index, val);
					ClassCastException enrichedException = new ClassCastException(errorMessage);
					enrichedException.setStackTrace(e.getStackTrace());
					throw enrichedException;
				}
			}
		};
	}
}
