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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.jdbc.JdbcDataType;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.MySQLToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.MySQLRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A built-in  {@link JdbcDialect} for MySQL.
 */
@Internal
public class MySQLDialect extends AbstractDialect {
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
	public LogicalType getInternalType(JdbcDataType externalType) {
		return null;
	}

	@Override
	public JdbcDataType getExternalType(LogicalType internalType) {
		return null;
	}

	@Override
	public JdbcRuntimeConverter getInputConverter(RowType rowType) {
		return new MySQLRuntimeConverter(rowType);
	}

	@Override
	public RowToJdbcConverter getOutputConverter(JdbcDataType[] jdbcDataTypes) {
		return new MySQLToJdbcConverter(jdbcDataTypes);
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
