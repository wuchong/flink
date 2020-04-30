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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Base class for {@link JdbcDialect}.
 */
abstract class AbstractDialect implements JdbcDialect {
	public abstract String dialectName();

	public abstract List<LogicalTypeRoot> unsupportedTypes();

	public abstract int maxDecimalPrecision();

	public abstract int minDecimalPrecision();

	public abstract int maxTimestampPrecision();

	public abstract int minTimestampPrecision();

	@Override
	public void validateInternalType(LogicalType type) {
		if (unsupportedTypes().contains(type.getTypeRoot()) ||
			(type) instanceof VarBinaryType && Integer.MAX_VALUE != ((VarBinaryType) type).getLength()) {
			throw new ValidationException(
				String.format("The %s dialect doesn't support type: %s.",
					dialectName(), type.toString()));
		}

		// only validate precision of DECIMAL type for blink planner
		if (type instanceof DecimalType) {
			int precision = ((DecimalType) type).getPrecision();
			if (precision > maxDecimalPrecision() || precision < minDecimalPrecision()) {
				throw new ValidationException(
					String.format("The precision of field type '%s' is out of the DECIMAL " +
							"precision range [%d, %d] supported by %s dialect.",
						type,
						minDecimalPrecision(),
						maxDecimalPrecision(),
						dialectName()));
			}
		}

		// only validate precision of DECIMAL type for blink planner
		if (type instanceof TimestampType) {
			int precision = ((TimestampType) type).getPrecision();
			if (precision > maxTimestampPrecision()
				|| precision < minTimestampPrecision()) {
				throw new ValidationException(
					String.format("The precision of field type '%s' is out of the TIMESTAMP " +
							"precision range [%d, %d] supported by %s dialect.",
						type,
						minTimestampPrecision(),
						maxTimestampPrecision(),
						dialectName()));
			}
		}
	}

	@Override
	public void validateExternalType(JdbcDataType type) {
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "\"" + identifier + "\"";
	}

	@Override
	public String getSelectFromStatement(String tableName, String[] selectFields, String[] conditionFields) {
		String selectExpressions = Arrays.stream(selectFields)
			.map(this::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String fieldExpressions = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "SELECT " + selectExpressions + " FROM " +
			quoteIdentifier(tableName) + (conditionFields.length > 0 ? " WHERE " + fieldExpressions : "");
	}

	@Override
	public String getInsertIntoStatement(String tableName, String[] fieldNames) {
		String columns = Arrays.stream(fieldNames)
			.map(this::quoteIdentifier)
			.collect(Collectors.joining(", "));
		String placeholders = Arrays.stream(fieldNames)
			.map(f -> "?")
			.collect(Collectors.joining(", "));
		return "INSERT INTO " + quoteIdentifier(tableName) +
			"(" + columns + ")" + " VALUES (" + placeholders + ")";
	}

	@Override
	public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
		String setClause = Arrays.stream(fieldNames)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(", "));
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "UPDATE " + quoteIdentifier(tableName) +
			" SET " + setClause +
			" WHERE " + conditionClause;
	}

	@Override
	public String getDeleteStatement(String tableName, String[] conditionFields) {
		String conditionClause = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + conditionClause;
	}

	@Override
	public String getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		return null;
	}

	@Override
	public String getRowExistsStatement(String tableName, String[] conditionFields) {
		String fieldExpressions = Arrays.stream(conditionFields)
			.map(f -> quoteIdentifier(f) + "=?")
			.collect(Collectors.joining(" AND "));
		return "SELECT 1 FROM " + quoteIdentifier(tableName) + " WHERE " + fieldExpressions;
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
