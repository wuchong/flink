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

package org.apache.flink.addons.hbase;

import org.apache.flink.HBaseBaseRowInputFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.DynamicTableSource;
import org.apache.flink.table.connectors.InputFormatChangelogReader;
import org.apache.flink.table.connectors.SupportsChangelogReading;
import org.apache.flink.table.connectors.SupportsProjectionPushDown;

/**
 * Creates a table source to scan an HBase table.
 */
public class HBaseDynamicTableSource implements DynamicTableSource,
		SupportsChangelogReading, SupportsProjectionPushDown {

	private final String tableName;
	private final org.apache.hadoop.conf.Configuration conf;
	private TableSchema tableSchema;

	public HBaseDynamicTableSource(org.apache.hadoop.conf.Configuration conf, TableSchema tableSchema, String tableName) {
		this.conf = conf;
		this.tableSchema = tableSchema;
		this.tableName = tableName;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ChangelogReader getChangelogReader(Context context) {
		HBaseTableSchema hbaseSchema = HBaseTableSchema.build(tableSchema);
		HBaseBaseRowInputFormat inputFormat = new HBaseBaseRowInputFormat(
			conf,
			tableName,
			hbaseSchema);
		return InputFormatChangelogReader.of(inputFormat);
	}

	@Override
	public String asSummaryString() {
		return "HBaseSource";
	}

	@Override
	public TableSchema applyProjection(TableSchema remainingSchema) {
		// supports nested projection
		this.tableSchema = remainingSchema;
		return remainingSchema;
	}
}
