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

import org.apache.flink.connectors.jdbc.JdbcLookupOptions;
import org.apache.flink.connectors.jdbc.JdbcOptions;
import org.apache.flink.connectors.jdbc.JdbcReadOptions;
import org.apache.flink.connectors.jdbc.JdbcTableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import static org.apache.flink.api.java.io.jdbc.JDBCTypeUtil.normalizeTableSchema;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link org.apache.flink.table.sources.TableSource} for JDBC.
 *
 * @deprecated Please use {@link JdbcTableSource}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCTableSource extends JdbcTableSource {

	private JDBCTableSource(
			JdbcOptions options,
			JdbcReadOptions readOptions,
			JdbcLookupOptions lookupOptions,
			TableSchema schema) {
		super(options, readOptions, lookupOptions, schema);
	}

	protected JDBCTableSource(
			JdbcOptions options,
			JdbcReadOptions readOptions,
			JdbcLookupOptions lookupOptions,
			TableSchema schema,
			int[] selectFields) {
		super(options, readOptions, lookupOptions, schema, selectFields);
	}

	@Override
	public TableSource<Row> projectFields(int[] fields) {
		return new JDBCTableSource(options, readOptions, lookupOptions, schema, fields);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JDBCTableSource}.
	 */
	public static class Builder extends JdbcTableSource.Builder {

		private JDBCOptions options;
		private JDBCReadOptions readOptions;
		private JDBCLookupOptions lookupOptions;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, scan related options.
		 * {@link JDBCReadOptions} will be only used for {@link StreamTableSource}.
		 */
		public Builder setReadOptions(JDBCReadOptions readOptions) {
			this.readOptions = readOptions;
			return this;
		}

		/**
		 * optional, lookup related options.
		 * {@link JDBCLookupOptions} only be used for {@link LookupableTableSource}.
		 */
		public Builder setLookupOptions(JDBCLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, table schema of this table source.
		 */
		public Builder setSchema(TableSchema schema) {
			this.schema = normalizeTableSchema(schema);
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JdbcTableSource
		 */
		public JDBCTableSource build() {
			checkNotNull(options, "No options supplied.");
			checkNotNull(schema, "No schema supplied.");
			if (readOptions == null) {
				readOptions = JDBCReadOptions.builder().build();
			}
			if (lookupOptions == null) {
				lookupOptions = JDBCLookupOptions.builder().build();
			}
			return new JDBCTableSource(options, readOptions, lookupOptions, schema);
		}
	}

}
