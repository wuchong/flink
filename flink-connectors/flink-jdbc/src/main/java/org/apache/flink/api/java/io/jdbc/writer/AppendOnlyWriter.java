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

package org.apache.flink.api.java.io.jdbc.writer;

import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.dataformat.ChangeType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * Just append record to jdbc, can not receive retract/delete message.
 */
public class AppendOnlyWriter implements JDBCWriter {

	private static final long serialVersionUID = 1L;

	private final String insertSQL;
	private final int[] fieldTypes;

	private transient PreparedStatement statement;

	public AppendOnlyWriter(String insertSQL, int[] fieldTypes) {
		this.insertSQL = insertSQL;
		this.fieldTypes = fieldTypes;
	}

	@Override
	public void open(Connection connection) throws SQLException {
		this.statement = connection.prepareStatement(insertSQL);
	}

	@Override
	public void addRecord(ChangeRow changeRow) throws SQLException {
		if (changeRow.getChangeType() != ChangeType.INSERT) {
			throw new IllegalArgumentException("Append mode can not receive upsert/delete message.");
		}
		setRecordToStatement(statement, fieldTypes, changeRow.getRow());
		statement.addBatch();
	}

	@Override
	public void executeBatch() throws SQLException {
		statement.executeBatch();
	}

	@Override
	public void close() throws SQLException {
		if (statement != null) {
			statement.close();
			statement = null;
		}
	}
}
