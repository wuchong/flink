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

package org.apache.flink.connectors.jdbc.sink;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCTypeUtil;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertSinkFunction;
import org.apache.flink.api.java.io.jdbc.JDBCUpsertTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.sinks.v2.DataWriterProvider;
import org.apache.flink.table.sinks.v2.SinkFunctionProvider;
import org.apache.flink.table.sinks.v2.TableSinkV2;
import org.apache.flink.table.sources.v2.UpdateMode;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.normalizeTableSchema;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * .
 */
public class JDBCTableSink implements TableSinkV2 {

	private final TableSchema schema;
	private final JDBCOptions options;
	private final int flushMaxSize;
	private final long flushIntervalMills;
	private final int maxRetryTime;
	private final String[] keyFields;

	private boolean isAppendOnly;

	public JDBCTableSink(TableSchema schema, JDBCOptions options, int flushMaxSize, long flushIntervalMills, int maxRetryTime, String[] keyFields) {
		this.schema = schema;
		this.options = options;
		this.flushMaxSize = flushMaxSize;
		this.flushIntervalMills = flushIntervalMills;
		this.maxRetryTime = maxRetryTime;
		this.keyFields = keyFields;
	}

	@Override
	public DataType getConsumedDataType() {
		return schema.toRowDataType().bridgedTo(ChangeRow.class);
	}

	@Override
	public UpdateMode getUpdateMode(boolean isAppendOnly) {
		this.isAppendOnly = isAppendOnly;
		if (isAppendOnly) {
			return UpdateMode.APPEND;
		} else {
			return UpdateMode.UPSERT;
		}
	}

	@Override
	public DataWriterProvider<?> createDataWriterProvider() {
		return SinkFunctionProvider.of(new JDBCUpsertSinkFunction(newFormat()));
	}

	private JDBCUpsertOutputFormat newFormat() {
		if (!isAppendOnly && (keyFields == null || keyFields.length == 0)) {
			throw new UnsupportedOperationException("Upsert mode JDBC sink requires to define a primary key on the table.");
		}

		// sql types
		int[] jdbcSqlTypes = Arrays.stream(schema.getFieldTypes())
			.mapToInt(JDBCTypeUtil::typeInformationToSqlType).toArray();

		return JDBCUpsertOutputFormat.builder()
			.setOptions(options)
			.setFieldNames(schema.getFieldNames())
			.setFlushMaxSize(flushMaxSize)
			.setFlushIntervalMills(flushIntervalMills)
			.setMaxRetryTimes(maxRetryTime)
			.setFieldTypes(jdbcSqlTypes)
			.setKeyFields(keyFields)
			.build();
	}

	public static Builder builder() {
		return new Builder();
	}

	// -----------------------------------------------------------------------------

	/**
	 * Builder for a {@link JDBCUpsertTableSink}.
	 */
	public static class Builder {
		private TableSchema schema;
		private JDBCOptions options;
		private int flushMaxSize = 5000;
		private long flushIntervalMills = 0;
		private int maxRetryTimes = 10;
		private List<String> keyFields;

		/**
		 * required, table schema of this table source.
		 */
		public Builder setTableSchema(TableSchema schema) {
			this.schema = normalizeTableSchema(schema);
			this.keyFields = schema.getPrimaryKey()
				.map(UniqueConstraint::getColumns)
				.orElse(Collections.emptyList());
			return this;
		}

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, flush max size (includes all append, upsert and delete records),
		 * over this number of records, will flush data.
		 */
		public Builder setFlushMaxSize(int flushMaxSize) {
			this.flushMaxSize = flushMaxSize;
			return this;
		}

		/**
		 * optional, flush interval mills, over this time, asynchronous threads will flush data.
		 */
		public Builder setFlushIntervalMills(long flushIntervalMills) {
			this.flushIntervalMills = flushIntervalMills;
			return this;
		}

		/**
		 * optional, max retry times for jdbc connector.
		 */
		public Builder setMaxRetryTimes(int maxRetryTimes) {
			this.maxRetryTimes = maxRetryTimes;
			return this;
		}

		public JDBCTableSink build() {
			checkNotNull(schema, "No schema supplied.");
			checkNotNull(options, "No options supplied.");
			return new JDBCTableSink(
				schema,
				options,
				flushMaxSize,
				flushIntervalMills,
				maxRetryTimes,
				keyFields.toArray(new String[0]));
		}
	}
}
