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
import org.apache.flink.connectors.jdbc.source.row.converter.PostgresToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.PostgresRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A built-in {@link JdbcDialect} for Postgres.
 */
@Internal
public class PostgresDialect extends AbstractDialect {
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
	public LogicalType getInternalType(JdbcDataType externalType) {
		return null;
	}

	@Override
	public JdbcDataType getExternalType(LogicalType internalType) {
		return null;
	}

	@Override
	public JdbcRuntimeConverter getInputConverter(RowType rowType) {
		return new PostgresRuntimeConverter(rowType);
	}

	@Override
	public RowToJdbcConverter getOutputConverter(JdbcDataType[] jdbcDataTypes) {
		return new PostgresToJdbcConverter(jdbcDataTypes);
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
