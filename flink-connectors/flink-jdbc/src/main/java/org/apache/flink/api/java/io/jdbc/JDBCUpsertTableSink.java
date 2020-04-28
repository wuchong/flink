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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.connectors.jdbc.JdbcOptions;
import org.apache.flink.connectors.jdbc.JdbcUpsertTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.UpsertStreamTableSink;

import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.normalizeTableSchema;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An upsert {@link UpsertStreamTableSink} for JDBC.
 */
@Deprecated
public class JDBCUpsertTableSink extends JdbcUpsertTableSink {

	private JDBCUpsertTableSink(
			TableSchema schema,
			JdbcOptions options,
			int flushMaxSize,
			long flushIntervalMills,
			int maxRetryTime) {
		super(schema, options, flushMaxSize, flushIntervalMills, maxRetryTime);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JDBCUpsertTableSink}.
	 */
	public static class Builder extends JdbcUpsertTableSink.Builder {
		private JDBCOptions options;

		/**
		 * required, table schema of this table source.
		 */
		public Builder setTableSchema(TableSchema schema) {
			this.schema = normalizeTableSchema(schema);
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

		public JDBCUpsertTableSink build() {
			checkNotNull(schema, "No schema supplied.");
			checkNotNull(options, "No options supplied.");
			return new JDBCUpsertTableSink(schema, options, flushMaxSize, flushIntervalMills, maxRetryTimes);
		}
	}
}
