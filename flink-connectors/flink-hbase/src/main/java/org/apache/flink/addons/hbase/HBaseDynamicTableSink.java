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

import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.sinks.DynamicTableSink;
import org.apache.flink.table.connectors.sinks.SinkFunctionChangelogWriter;
import org.apache.flink.table.connectors.sinks.SupportsChangelogWriting;
import org.apache.flink.table.datastructures.RowKind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;

public class HBaseDynamicTableSink implements DynamicTableSink, SupportsChangelogWriting {

	private final HBaseTableSchema hbaseTableSchema;
	private final HBaseOptions hbaseOptions;
	private final HBaseWriteOptions writeOptions;

	public HBaseDynamicTableSink(
			HBaseTableSchema hbaseTableSchema,
			HBaseOptions hbaseOptions,
			HBaseWriteOptions writeOptions) {
		this.hbaseTableSchema = hbaseTableSchema;
		this.hbaseOptions = hbaseOptions;
		this.writeOptions = writeOptions;
	}

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
		if (requestedMode.isInsertOnly()) {
			return requestedMode;
		} else {
			return ChangelogMode.newBuilder()
				.addSupportedKind(RowKind.INSERT)
				.addSupportedKind(RowKind.DELETE)
				.addSupportedKind(RowKind.UPDATE_AFTER)
				.build();
		}
	}

	@Override
	public ChangelogWriter getChangelogWriter(ChangelogWriterContext context) {
		Configuration hbaseClientConf = HBaseConfiguration.create();
		hbaseClientConf.set(HConstants.ZOOKEEPER_QUORUM, hbaseOptions.getZkQuorum());
		hbaseOptions.getZkNodeParent().ifPresent(v -> hbaseClientConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, v));
		HBaseSinkFunction sinkFunction = new HBaseSinkFunction(
			hbaseOptions.getTableName(),
			hbaseTableSchema,
			hbaseClientConf,
			writeOptions.getBufferFlushMaxSizeInBytes(),
			writeOptions.getBufferFlushMaxRows(),
			writeOptions.getBufferFlushIntervalMillis());
		return SinkFunctionChangelogWriter.of(sinkFunction);
	}

	@Override
	public String asSummaryString() {
		return "HBaseSink";
	}

}
