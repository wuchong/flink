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

import org.apache.flink.connectors.jdbc.JdbcBatchingOutputFormat;
import org.apache.flink.connectors.jdbc.JdbcConnectionOptions;
import org.apache.flink.connectors.jdbc.JdbcConnectionProvider;
import org.apache.flink.connectors.jdbc.JdbcExecutionOptions;
import org.apache.flink.connectors.jdbc.JdbcInsertOptions;
import org.apache.flink.connectors.jdbc.JdbcOutputFormat;
import org.apache.flink.connectors.jdbc.SimpleJdbcConnectionProvider;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @deprecated please use {@link JdbcBatchingOutputFormat}
 */
@Deprecated
public class JDBCOutputFormat extends JdbcOutputFormat {

	/**
	 * @deprecated use {@link JDBCOutputFormatBuilder builder} instead.
	 */
	@Deprecated
	public JDBCOutputFormat(String username, String password, String drivername, String dbURL, String query, int batchInterval, int[] typesArray) {
		super(username, password, drivername, dbURL, query, batchInterval, typesArray);
	}

	private JDBCOutputFormat(JdbcConnectionProvider connectionProvider, JdbcInsertOptions insertOptions, JdbcExecutionOptions batchOptions) {
		super(connectionProvider, insertOptions, batchOptions);
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

	/**
	 * Builder for {@link JDBCOutputFormat}.
	 */
	public static class JDBCOutputFormatBuilder extends JdbcOutputFormatBuilder {

		private JDBCOutputFormatBuilder() {
		}

		public JDBCOutputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JDBCOutputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JDBCOutputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JDBCOutputFormatBuilder setQuery(String query) {
			this.query = query;
			return this;
		}

		public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
			this.batchInterval = batchInterval;
			return this;
		}

		public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
			this.typesArray = typesArray;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JdbcOutputFormat
		 */
		public JDBCOutputFormat finish() {
			return new JDBCOutputFormat(
				new SimpleJdbcConnectionProvider(buildConnectionOptions()),
				new JdbcInsertOptions(query, typesArray),
				JdbcExecutionOptions.builder().withBatchSize(batchInterval).build());
		}

		public JdbcConnectionOptions buildConnectionOptions() {
			if (this.username == null) {
				LOG.info("Username was not supplied.");
			}
			if (this.password == null) {
				LOG.info("Password was not supplied.");
			}

			return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(dbURL)
				.withDriverName(drivername)
				.withUsername(username)
				.withPassword(password)
				.build();
		}
	}
}
