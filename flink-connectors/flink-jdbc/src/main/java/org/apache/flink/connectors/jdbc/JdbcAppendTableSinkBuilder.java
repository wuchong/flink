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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.connectors.jdbc.AbstractJdbcOutputFormat.DEFAULT_FLUSH_MAX_SIZE;


/**
 * A builder to configure and build the JdbcAppendTableSink.
 */
public class JdbcAppendTableSinkBuilder {
	protected String username;
	protected String password;
	protected String driverName;
	protected String dbURL;
	protected String query;
	protected int batchSize = DEFAULT_FLUSH_MAX_SIZE;
	private JdbcType[] parameterTypes;

	/**
	 * Specify the username of the JDBC connection.
	 * @param username the username of the JDBC connection.
	 */
	public JdbcAppendTableSinkBuilder setUsername(String username) {
		this.username = username;
		return this;
	}

	/**
	 * Specify the password of the JDBC connection.
	 * @param password the password of the JDBC connection.
	 */
	public JdbcAppendTableSinkBuilder setPassword(String password) {
		this.password = password;
		return this;
	}

	/**
	 * Specify the name of the JDBC driver that will be used.
	 * @param drivername the name of the JDBC driver.
	 */
	public JdbcAppendTableSinkBuilder setDrivername(String drivername) {
		this.driverName = drivername;
		return this;
	}

	/**
	 * Specify the URL of the JDBC database.
	 * @param dbURL the URL of the database, whose format is specified by the
	 *              corresponding JDBC driver.
	 */
	public JdbcAppendTableSinkBuilder setDBUrl(String dbURL) {
		this.dbURL = dbURL;
		return this;
	}

	/**
	 * Specify the query that the sink will execute. Usually user can specify
	 * INSERT, REPLACE or UPDATE to push the data to the database.
	 * @param query The query to be executed by the sink.
	 * @see JdbcOutputFormat.JdbcOutputFormatBuilder#setQuery(String)
	 */
	public JdbcAppendTableSinkBuilder setQuery(String query) {
		this.query = query;
		return this;
	}

	/**
	 * Specify the size of the batch. By default the sink will batch the query
	 * to improve the performance
	 * @param batchSize the size of batch
	 */
	public JdbcAppendTableSinkBuilder setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	/**
	 * Specify the type of the rows that the sink will be accepting.
	 * @param types the type of each field
	 */
	public JdbcAppendTableSinkBuilder setParameterTypes(TypeInformation<?>... types) {
		JdbcType[] ty = new JdbcType[types.length];
		for (int i = 0; i < types.length; ++i) {
			ty[i] = JdbcTypeUtil.typeInformationToJdbcType(types[i]);
		}
		this.parameterTypes = ty;
		return this;
	}

	/**
	 * Specify the type of the rows that the sink will be accepting.
	 * @param types the type of each field defined by {@see java.sql.Types}.
	 */
	public JdbcAppendTableSinkBuilder setParameterTypes(JdbcType... types) {
		this.parameterTypes = types;
		return this;
	}

	/**
	 * Finalizes the configuration and checks validity.
	 *
	 * @return Configured JdbcOutputFormat
	 */
	public JdbcAppendTableSink build() {
		Preconditions.checkNotNull(parameterTypes,
			"Types of the query parameters are not specified." +
			" Please specify types using the setParameterTypes() method.");

		JdbcOutputFormat format = JdbcOutputFormat.buildJdbcOutputFormat()
			.setUsername(username)
			.setPassword(password)
			.setDBUrl(dbURL)
			.setQuery(query)
			.setDrivername(driverName)
			.setBatchInterval(batchSize)
			.setSqlTypes(parameterTypes)
			.finish();

		return new JdbcAppendTableSink(format);
	}
}
