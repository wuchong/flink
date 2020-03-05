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

import org.apache.flink.addons.hbase.util.HBaseSerde;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.dataformats.ChangelogKind;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;

public class HBaseSinkFunction extends AbstractHBaseSinkFunction<BaseRow> {

	private static final long serialVersionUID = -4440024315087689066L;

	private final HBaseSerde serde;

	public HBaseSinkFunction(
			String hTableName,
			HBaseTableSchema schema,
			Configuration conf,
			long bufferFlushMaxSizeInBytes,
			long bufferFlushMaxMutations,
			long bufferFlushIntervalMillis) {
		super(hTableName, schema, conf, bufferFlushMaxSizeInBytes, bufferFlushMaxMutations, bufferFlushIntervalMillis);
		this.serde = new HBaseSerde(schema);
	}

	@Override
	public Mutation createMutation(BaseRow row) {
		if (row.getChangelogKind() == ChangelogKind.INSERT
				|| row.getChangelogKind() == ChangelogKind.UPDATE_AFTER) {
			// INSERT or UPDATE_AFTER
			return serde.createPutMutation(row);
		} else {
			// DELETE or UPDATE_BEFORE
			return serde.createDeleteMutation(row);
		}
	}
}
