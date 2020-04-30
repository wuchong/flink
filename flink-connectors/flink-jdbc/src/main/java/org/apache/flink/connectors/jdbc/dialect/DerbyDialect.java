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
import org.apache.flink.connectors.jdbc.source.row.converter.DerbyToJdbcConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.DerbyRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.JdbcRuntimeConverter;
import org.apache.flink.connectors.jdbc.source.row.converter.RowToJdbcConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;

/**
 * A built-in {@link JdbcDialect} for Derby.
 */
@Internal
public class DerbyDialect extends AbstractDialect {
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
	public LogicalType getInternalType(JdbcDataType externalType) {
		return null;
	}

	@Override
	public JdbcDataType getExternalType(LogicalType internalType) {
		return null;
	}

	@Override
	public JdbcRuntimeConverter getInputConverter(RowType rowType) {
		return new DerbyRuntimeConverter(rowType);
	}

	@Override
	public RowToJdbcConverter getOutputConverter(JdbcDataType[] jdbcDataTypes) {
		return new DerbyToJdbcConverter(jdbcDataTypes);
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
