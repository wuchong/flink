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

package org.apache.flink.formats.json;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.ChangelogSerializationSchema;
import org.apache.flink.table.datastructures.RowData;
import org.apache.flink.table.datastructures.RowKind;
import org.apache.flink.table.datastructures.GenericRowData;
import org.apache.flink.table.datastructures.StringData;
import org.apache.flink.table.types.logical.RowType;

public class DebeziumJsonSerializationSchema implements ChangelogSerializationSchema {
	private static final long serialVersionUID = 1L;

	private static final StringData OP_CREATE = StringData.fromString("c");
	private static final StringData OP_DELETE = StringData.fromString("d");

	private final JsonBaseRowSerializationSchema jsonSerializer;

	public DebeziumJsonSerializationSchema(TableSchema schema) {
		this.jsonSerializer = new JsonBaseRowSerializationSchema(createJsonRowType(schema));
	}

	@Override
	public ChangelogMode supportedChangelogMode() {
		// only supports INSERT and DELETE, request the planner to convert updates to insert+delete
		return ChangelogMode.newBuilder()
			.addSupportedKind(RowKind.INSERT)
			.addSupportedKind(RowKind.DELETE)
			.build();
	}

	@Override
	public byte[] serialize(RowData row) {
		// construct a Debezium record
		GenericRowData debezium = new GenericRowData(5);
		debezium.setField(2, "TODO"); // source field
		debezium.setField(4, System.currentTimeMillis()); // ts_ms field
		if (row.getChangelogKind() == RowKind.INSERT) {
			debezium.setField(0, null); // before field
			debezium.setField(1, row); // after field
			debezium.setField(3, OP_CREATE);  // op field
		} else if (row.getChangelogKind() == RowKind.DELETE) {
			debezium.setField(0, row); // before field
			debezium.setField(1, null); // after field
			debezium.setField(3, OP_DELETE);  // op field
		} else {
			throw new IllegalStateException("Unsupported changelog kind: " + row.getChangelogKind());
		}
		return jsonSerializer.serialize(debezium);
	}

	private RowType createJsonRowType(TableSchema schema) {
		TableSchema dbzSchema = TableSchema.builder()
			.field("before", schema.toRowDataType())
			.field("after", schema.toRowDataType())
			.field("source", DataTypes.STRING())
			.field("op", DataTypes.STRING())
			.field("ts_ms", DataTypes.BIGINT())
			.build();
		return (RowType) dbzSchema.toRowDataType().getLogicalType();
	}
}
