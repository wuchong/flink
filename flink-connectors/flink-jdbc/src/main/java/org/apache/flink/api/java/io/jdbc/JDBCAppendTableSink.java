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

import org.apache.flink.connectors.jdbc.JdbcAppendTableSink;

/**
 * An at-least-once Table sink for JDBC.
 *
 * <p>The mechanisms of Flink guarantees delivering messages at-least-once to this sink (if
 * checkpointing is enabled). However, one common use case is to run idempotent queries
 * (e.g., <code>REPLACE</code> or <code>INSERT OVERWRITE</code>) to upsert into the database and
 * achieve exactly-once semantic.</p>
 *
 * @deprecated Please use {@link JdbcAppendTableSink}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCAppendTableSink extends JdbcAppendTableSink {

	JDBCAppendTableSink(JDBCOutputFormat outputFormat) {
		super(outputFormat);
	}

	public static JDBCAppendTableSinkBuilder builder() {
		return new JDBCAppendTableSinkBuilder();
	}
}
