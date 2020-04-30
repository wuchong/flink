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

package org.apache.flink.connectors.jdbc;

import org.apache.flink.connectors.jdbc.dialect.JdbcDialectService;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */

/**
 * @deprecated use {@link JdbcBatchingOutputFormat}
 */
@Deprecated
public class JdbcOutputFormat extends AbstractJdbcOutputFormat<Row> {

	private static final long serialVersionUID = 1L;

	protected static final Logger LOG = LoggerFactory.getLogger(JdbcOutputFormat.class);

	final JdbcInsertOptions insertOptions;
	private final JdbcExecutionOptions batchOptions;

	private transient PreparedStatement upload;
	private transient int batchCount = 0;

	/**
	 * @deprecated use {@link JdbcOutputFormatBuilder builder} instead.
	 */
	@Deprecated
	public JdbcOutputFormat(String username, String password, String drivername, String dbURL, String query, int batchInterval, JdbcDataType[] typesArray) {
		this(new SimpleJdbcConnectionProvider(new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(dbURL).withDriverName(drivername).withUsername(username).withPassword(password).build()),
				new JdbcInsertOptions(JdbcDialectService.get(dbURL).get(), query, typesArray),
				JdbcExecutionOptions.builder().withBatchSize(batchInterval).build());
	}

	protected JdbcOutputFormat(JdbcConnectionProvider connectionProvider, JdbcInsertOptions insertOptions, JdbcExecutionOptions batchOptions) {
		super(connectionProvider);
		this.insertOptions = insertOptions;
		this.batchOptions = batchOptions;
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an
	 * I/O problem.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		super.open(taskNumber, numTasks);
		try {
			upload = connection.prepareStatement(insertOptions.getQuery());
		} catch (SQLException sqe) {
			throw new IOException("open() failed.", sqe);
		}
	}

	@Override
	public void writeRecord(Row row) {
		try {
			insertOptions.getDialect().getOutputConverter(insertOptions.getFieldTypes())
				.toExternal(row, upload);
			upload.addBatch();
		} catch (SQLException e) {
			throw new RuntimeException("Preparation of JDBC statement failed.", e);
		}

		batchCount++;

		if (batchCount >= batchOptions.getBatchSize()) {
			// execute batch
			flush();
		}
	}

	@Override
	public void flush() {
		try {
			upload.executeBatch();
			batchCount = 0;
		} catch (SQLException e) {
			throw new RuntimeException("Execution of JDBC statement failed.", e);
		}
	}

	JdbcDataType[] getTypesArray() {
		return insertOptions.getFieldTypes();
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 */
	@Override
	public void close() {
		if (upload != null) {
			flush();
			try {
				upload.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				upload = null;
			}
		}

		super.close();
	}

	public static JdbcOutputFormatBuilder buildJdbcOutputFormat() {
		return new JdbcOutputFormatBuilder();
	}

	public JdbcDataType[] getFieldTypes() {
		return insertOptions.getFieldTypes();
	}

	/**
	 * Builder for {@link JdbcOutputFormat}.
	 */
	public static class JdbcOutputFormatBuilder {
		protected String username;
		protected String password;
		protected String drivername;
		protected String dbURL;
		protected String query;
		protected int batchInterval = DEFAULT_FLUSH_MAX_SIZE;
		protected JdbcDataType[] typesArray;

		protected JdbcOutputFormatBuilder() {}

		public JdbcOutputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JdbcOutputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JdbcOutputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JdbcOutputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JdbcOutputFormatBuilder setQuery(String query) {
			this.query = query;
			return this;
		}

		public JdbcOutputFormatBuilder setBatchInterval(int batchInterval) {
			this.batchInterval = batchInterval;
			return this;
		}

		public JdbcOutputFormatBuilder setSqlTypes(JdbcDataType[] typesArray) {
			this.typesArray = typesArray;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JdbcOutputFormat
		 */
		public JdbcOutputFormat finish() {
			return new JdbcOutputFormat(
				new SimpleJdbcConnectionProvider(buildConnectionOptions()),
				new JdbcInsertOptions(
					JdbcDialectService.get(dbURL).orElseThrow(() ->
						new IllegalArgumentException(String.format("Can not handle the db url: %s", dbURL))),
					query,
					typesArray),
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
