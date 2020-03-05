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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.dataformats.ChangelogKind;
import org.apache.flink.table.dataformats.GenericRow;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.io.IOException;

public class DebeziumJsonDeserializationSchema implements ChangelogDeserializationSchema {
	private static final long serialVersionUID = 1L;

	private static final String OP_CREATE = "c";
	private static final String OP_UPDATE = "u";
	private static final String OP_DELETE = "d";

	private final JsonBaseRowDeserializationSchema jsonDeserializer;
	/** logical type describing the result type. **/
	private final TableSchema schema;

	public DebeziumJsonDeserializationSchema(TableSchema schema) {
		this.schema = schema;
		this.jsonDeserializer = new JsonBaseRowDeserializationSchema(createJsonRowType(schema));
	}

	@Override
	public BaseRow deserialize(byte[] message) throws IOException {
		GenericRow row = (GenericRow) jsonDeserializer.deserialize(message);
		GenericRow before = (GenericRow) row.getField(0);
		GenericRow after = (GenericRow) row.getField(1);
		String op = row.getField(2).toString();
		if (OP_CREATE.equals(op)){
			after.setChangelogKind(ChangelogKind.INSERT);
			return after;
		} if (OP_UPDATE.equals(op)) {
			after.setChangelogKind(ChangelogKind.UPDATE_AFTER);
			return after;
		} else if (OP_DELETE.equals(op)) {
			before.setChangelogKind(ChangelogKind.DELETE);
			return before;
		} else {
			throw new RuntimeException("Unsupported operation type: " + op);
		}
	}

	@Override
	public boolean isEndOfStream(BaseRow nextElement) {
		return false;
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

	@Override
	public TypeInformation<BaseRow> getProducedType() {
		RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
		return new BaseRowTypeInfo(rowType);
	}

	@Override
	public ChangelogMode producedChangelogMode() {
		return ChangelogMode.newBuilder()
			.addSupportedKind(ChangelogKind.INSERT)
			.addSupportedKind(ChangelogKind.UPDATE_AFTER)
			.addSupportedKind(ChangelogKind.DELETE)
			.build();
	}
}
