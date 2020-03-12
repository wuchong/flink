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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogSerializationSchema;
import org.apache.flink.table.datastructures.RowData;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.ChangelogDeserializationSchemaFactory;
import org.apache.flink.table.factories.ChangelogSerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of JSON-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
@PublicEvolving
public class JsonBaseRowFormatFactory extends TableFormatFactoryBase<RowData>
		implements ChangelogDeserializationSchemaFactory, ChangelogSerializationSchemaFactory {

	public JsonBaseRowFormatFactory() {
		super(JsonValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	protected List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(JsonValidator.FORMAT_JSON_SCHEMA);
		properties.add(JsonValidator.FORMAT_SCHEMA);
		properties.add(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD);
		return properties;
	}

	@Override
	public ChangelogDeserializationSchema createDeserializationSchema(Map<String, String> properties) {
		TableSchema schema = deriveSchema(properties);
		RowType logicalType = (RowType) schema.toRowDataType().getLogicalType();
		return new JsonBaseRowDeserializationSchema(logicalType);
	}

	@Override
	public ChangelogSerializationSchema createSerializationSchema(Map<String, String> properties) {
		TableSchema schema = deriveSchema(properties);
		RowType logicalType = (RowType) schema.toRowDataType().getLogicalType();
		return new JsonBaseRowSerializationSchema(logicalType);
	}
}
