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

import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialect;
import org.apache.flink.api.java.io.jdbc.dialect.JDBCDialects;
import org.apache.flink.connectors.jdbc.JdbcOptions;
import org.apache.flink.connectors.jdbc.dialect.JdbcDialect;

import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Options for the JDBC connector.
 *
 * @deprecated Please use {@link JdbcOptions}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCOptions extends JdbcOptions {
	private JDBCOptions(String dbURL, String tableName, String driverName, String username, String password, JdbcDialect dialect) {
		super(dbURL, tableName, driverName, username, password, dialect);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link JDBCOptions}.
	 */
	public static class Builder extends JdbcOptions.Builder {

		protected JDBCDialect dialect;

		/**
		 * required, table name.
		 */
		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * optional, user name.
		 */
		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * optional, password.
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * optional, driver name, dialect has a default driver name,
		 * See {@link JdbcDialect#defaultDriverName}.
		 */
		public Builder setDriverName(String driverName) {
			this.driverName = driverName;
			return this;
		}

		/**
		 * required, JDBC DB url.
		 */
		public Builder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		/**
		 * optional, Handle the SQL dialect of jdbc driver. If not set, it will be infer by
		 * {@link JDBCDialects#get} from DB url.
		 */
		public Builder setDialect(JDBCDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		public JDBCOptions build() {
			checkNotNull(dbURL, "No dbURL supplied.");
			checkNotNull(tableName, "No tableName supplied.");
			if (this.dialect == null) {
				Optional<JDBCDialect> optional = JDBCDialects.get(dbURL);
				this.dialect = optional.orElseGet(() -> {
					throw new NullPointerException("No dialect supplied.");
				});
			}
			if (this.driverName == null) {
				Optional<String> optional = dialect.defaultDriverName();
				this.driverName = optional.orElseGet(() -> {
					throw new NullPointerException("No driverName supplied.");
				});
			}

			return new JDBCOptions(dbURL, tableName, driverName, username, password, dialect);		}
	}
}
