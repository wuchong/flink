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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.dataformat.ChangeType;
import org.apache.flink.table.typeutils.ChangeRowTypeInfo;
import org.apache.flink.types.Row;

import java.io.IOException;

/**
 * .
 */
public class DebeziumJsonDeserializationSchema implements DeserializationSchema<ChangeRow> {
	private static final long serialVersionUID = 1L;

	private static final String OP_CREATE = "c";
	private static final String OP_UPDATE = "u";
	private static final String OP_DELETE = "d";

	private final JsonRowDeserializationSchema jsonDeserializer;
	private final ChangeRowTypeInfo resultType;

	public DebeziumJsonDeserializationSchema(JsonRowDeserializationSchema jsonDeserializer, ChangeRowTypeInfo resultType) {
		this.jsonDeserializer = jsonDeserializer;
		this.resultType = resultType;
	}

	@Override
	public ChangeRow deserialize(byte[] message) throws IOException {
		Row row = jsonDeserializer.deserialize(message);
		Row before = (Row) row.getField(0);
		Row after = (Row) row.getField(1);
		String op = (String) row.getField(2);
		if (OP_CREATE.equals(op)){
			return new ChangeRow(ChangeType.INSERT, after);
		} if (OP_UPDATE.equals(op)) {
			return new ChangeRow(ChangeType.UPDATE_NEW, after);
		} else if (OP_DELETE.equals(op)) {
			return new ChangeRow(ChangeType.DELETE, before);
		} else {
			throw new RuntimeException("Unsupported operation type: " + op);
		}
	}

	@Override
	public boolean isEndOfStream(ChangeRow nextElement) {
		return false;
	}

	@Override
	public TypeInformation<ChangeRow> getProducedType() {
		return resultType;
	}
}
