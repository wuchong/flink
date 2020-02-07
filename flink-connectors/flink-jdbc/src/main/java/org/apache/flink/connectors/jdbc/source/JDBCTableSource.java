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

package org.apache.flink.connectors.jdbc.source;

import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.api.java.io.jdbc.JDBCReadOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.sources.v2.DataReaderProvider;
import org.apache.flink.table.sources.v2.DynamicTableType;
import org.apache.flink.table.sources.v2.TableSourceV2;
import org.apache.flink.table.sources.v2.UpdateMode;
import org.apache.flink.table.types.DataType;

public class JDBCTableSource implements TableSourceV2 {

	private final JDBCOptions options;
	private final JDBCReadOptions readOptions;
	private final TableSchema schema;

	public JDBCTableSource(JDBCOptions options, JDBCReadOptions readOptions, TableSchema schema) {
		this.options = options;
		this.readOptions = readOptions;
		this.schema = schema;
	}

	@Override
	public DynamicTableType getDynamicTableType() {
		return DynamicTableType.TABLE;
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public DataType getProducedDataType() {
		return schema.toRowDataType().bridgedTo(ChangeRow.class);
	}

	@Override
	public UpdateMode getProducedUpdateMode() {
		return null;
	}

	@Override
	public DataReaderProvider<?> createReader() {
		return null;
	}
}
