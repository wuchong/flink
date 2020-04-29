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

import org.apache.flink.connectors.jdbc.source.row.converter.DerbyToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.DerbyToRowConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcToRowConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.MySQLToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.MySQLToRowConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.PostgresToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.PostgresToRowConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Built in JDBC dialects.
 */
public final class JdbcDialects {

	private static final List<JdbcDialect> DIALECTS = Arrays.asList(
		new DerbyDialect(),
		new MySQLDialect(),
		new PostgresDialect()
	);

	/**
	 * Fetch the JdbcDialect class corresponding to a given database url.
	 */
	public static Optional<JdbcDialect> get(String url) {
		for (JdbcDialect dialect : DIALECTS) {
			if (dialect.canHandle(url)) {
				return Optional.of(dialect);
			}
		}
		return Optional.empty();
	}

	/**
	 * Base class for {@link JdbcDialect}.
	 */
	private abstract static class AbstractDialect implements JdbcDialect {

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
		public void validateExternalType(JdbcType type) {
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
		public String getCreateTableStatement(String tableName, String[] fieldNames, JdbcType[] types, String[] uniqueKeyFields) {
			return null;
		}
	}

	private static class DerbyDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to derby docs:
		// http://db.apache.org/derby/docs/10.14/ref/rrefsqlj27620.html
		private static final int MAX_TIMESTAMP_PRECISION = 9;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of DECIMAL type according to derby docs:
		// http://db.apache.org/derby/docs/10.14/ref/rrefsqlj15260.html
		private static final int MAX_DECIMAL_PRECISION = 31;
		private static final int MIN_DECIMAL_PRECISION = 1;

		@Override
		public String dialectName() {
			return "derby";
		}

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:derby:");
		}

		@Override
		public String defaultDriverName() {
			return "org.apache.derby.jdbc.EmbeddedDriver";
		}

		@Override
		public LogicalType getInternalType(JdbcType externalType) {
			return null;
		}

		@Override
		public JdbcType getExternalType(LogicalType internalType) {
			return null;
		}

		@Override
		public JdbcToRowConverter getInputConverter(RowType rowType) {
			return new DerbyToRowConverter(rowType);
		}

		@Override
		public RowToJdbcConverter getOutputConverter(JdbcType[] jdbcTypes) {
			return new DerbyToJdbcConverter(jdbcTypes);
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return identifier;
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			return Arrays.asList(
				LogicalTypeRoot.BINARY,
				LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
				LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
				LogicalTypeRoot.INTERVAL_YEAR_MONTH,
				LogicalTypeRoot.INTERVAL_DAY_TIME,
				LogicalTypeRoot.ARRAY,
				LogicalTypeRoot.MULTISET,
				LogicalTypeRoot.MAP,
				LogicalTypeRoot.ROW,
				LogicalTypeRoot.DISTINCT_TYPE,
				LogicalTypeRoot.STRUCTURED_TYPE,
				LogicalTypeRoot.NULL,
				LogicalTypeRoot.RAW,
				LogicalTypeRoot.SYMBOL,
				LogicalTypeRoot.UNRESOLVED);
		}
	}

	/**
	 * MySQL dialect.
	 */
	public static class MySQLDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
		// https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
		private static final int MAX_TIMESTAMP_PRECISION = 6;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of DECIMAL type according to Mysql docs:
		// https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
		private static final int MAX_DECIMAL_PRECISION = 65;
		private static final int MIN_DECIMAL_PRECISION = 1;

		@Override
		public String dialectName() {
			return "mysql";
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			return Arrays.asList(
				LogicalTypeRoot.BINARY,
				LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
				LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
				LogicalTypeRoot.INTERVAL_YEAR_MONTH,
				LogicalTypeRoot.INTERVAL_DAY_TIME,
				LogicalTypeRoot.ARRAY,
				LogicalTypeRoot.MULTISET,
				LogicalTypeRoot.MAP,
				LogicalTypeRoot.ROW,
				LogicalTypeRoot.DISTINCT_TYPE,
				LogicalTypeRoot.STRUCTURED_TYPE,
				LogicalTypeRoot.NULL,
				LogicalTypeRoot.RAW,
				LogicalTypeRoot.SYMBOL,
				LogicalTypeRoot.UNRESOLVED);
		}

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:mysql:");
		}

		@Override
		public String defaultDriverName() {
			return "com.mysql.jdbc.Driver";
		}

		@Override
		public LogicalType getInternalType(JdbcType externalType) {
			return null;
		}

		@Override
		public JdbcType getExternalType(LogicalType internalType) {
			return null;
		}

		@Override
		public JdbcToRowConverter getInputConverter(RowType rowType) {
			return new MySQLToRowConverter(rowType);
		}

		@Override
		public RowToJdbcConverter getOutputConverter(JdbcType[] jdbcTypes) {
			return new MySQLToJdbcConverter(jdbcTypes);
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return "`" + identifier + "`";
		}

		/**
		 * Mysql upsert query use DUPLICATE KEY UPDATE.
		 *
		 * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
		 *
		 * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
		 */
		@Override
		public String getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String updateClause = Arrays.stream(fieldNames)
				.map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
				.collect(Collectors.joining(", "));
			return getInsertIntoStatement(tableName, fieldNames) +
				" ON DUPLICATE KEY UPDATE " + updateClause;
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}
	}

	/**
	 * Postgres dialect.
	 */
	public static class PostgresDialect extends AbstractDialect {

		private static final long serialVersionUID = 1L;

		// Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
		// https://www.postgresql.org/docs/12/datatype-datetime.html
		private static final int MAX_TIMESTAMP_PRECISION = 6;
		private static final int MIN_TIMESTAMP_PRECISION = 1;

		// Define MAX/MIN precision of TIMESTAMP type according to PostgreSQL docs:
		// https://www.postgresql.org/docs/12/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
		private static final int MAX_DECIMAL_PRECISION = 1000;
		private static final int MIN_DECIMAL_PRECISION = 1;

		@Override
		public String dialectName() {
			return "postgresql";
		}

		@Override
		public List<LogicalTypeRoot> unsupportedTypes() {
			return Arrays.asList(
				LogicalTypeRoot.BINARY,
				LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
				LogicalTypeRoot.INTERVAL_YEAR_MONTH,
				LogicalTypeRoot.INTERVAL_DAY_TIME,
				LogicalTypeRoot.MULTISET,
				LogicalTypeRoot.MAP,
				LogicalTypeRoot.ROW,
				LogicalTypeRoot.DISTINCT_TYPE,
				LogicalTypeRoot.STRUCTURED_TYPE,
				LogicalTypeRoot.NULL,
				LogicalTypeRoot.RAW,
				LogicalTypeRoot.SYMBOL,
				LogicalTypeRoot.UNRESOLVED);
		}

		@Override
		public boolean canHandle(String url) {
			return url.startsWith("jdbc:postgresql:");
		}

		@Override
		public String defaultDriverName() {
			return "org.postgresql.Driver";
		}

		@Override
		public LogicalType getInternalType(JdbcType externalType) {
			return null;
		}

		@Override
		public JdbcType getExternalType(LogicalType internalType) {
			return null;
		}

		@Override
		public JdbcToRowConverter getInputConverter(RowType rowType) {
			return new PostgresToRowConverter(rowType);
		}

		@Override
		public RowToJdbcConverter getOutputConverter(JdbcType[] jdbcTypes) {
			return new PostgresToJdbcConverter(jdbcTypes);
		}

		/**
		 * Postgres upsert query. It use ON CONFLICT ... DO UPDATE SET.. to replace into Postgres.
		 */
		@Override
		public String getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
			String uniqueColumns = Arrays.stream(uniqueKeyFields)
				.map(this::quoteIdentifier)
				.collect(Collectors.joining(", "));
			String updateClause = Arrays.stream(fieldNames)
				.map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
				.collect(Collectors.joining(", "));
			return getInsertIntoStatement(tableName, fieldNames) +
				" ON CONFLICT (" + uniqueColumns + ")" +
				" DO UPDATE SET " + updateClause;
		}

		@Override
		public String quoteIdentifier(String identifier) {
			return identifier;
		}

		@Override
		public int maxDecimalPrecision() {
			return MAX_DECIMAL_PRECISION;
		}

		@Override
		public int minDecimalPrecision() {
			return MIN_DECIMAL_PRECISION;
		}

		@Override
		public int maxTimestampPrecision() {
			return MAX_TIMESTAMP_PRECISION;
		}

		@Override
		public int minTimestampPrecision() {
			return MIN_TIMESTAMP_PRECISION;
		}

	}
}
