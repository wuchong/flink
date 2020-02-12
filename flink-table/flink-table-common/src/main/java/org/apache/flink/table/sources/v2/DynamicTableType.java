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

package org.apache.flink.table.sources.v2;

/**
 * .
 */
public enum DynamicTableType {
	/**
	 * The Table representation of dynamic table. Table representation is a sequence of tables that
	 * change over time. Until there is no further improvement, Flink, like a database,
	 * temporarily only supports the ability to access the full amount of data of a certain
	 * version of the table, such as always accessing the current version of the table.
	 * And when accessing a snapshot of a table, Flink assumes that the data is always bounded.
	 * Typical scenarios include database tables, tables in warehouses, file systems
	 * with various formats.
	 */
	TABLE,

	/**
	 * The Stream representation of dynamic table. Stream representation is a sequence of messages.
	 * The messages here may include INSERT and UPDATE and DELETE messages. We generally consider
	 * streams to be unbounded, with bounded data as a special case. Typical usage scenarios
	 * include: database binlog, message queue, etc.
	 */
	STREAM
}
