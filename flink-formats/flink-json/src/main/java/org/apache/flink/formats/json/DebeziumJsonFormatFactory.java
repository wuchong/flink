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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.typeutils.ChangeRowTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * .
 */
public class DebeziumJsonFormatFactory
	extends TableFormatFactoryBase<ChangeRow> implements DeserializationSchemaFactory<ChangeRow> {

	public DebeziumJsonFormatFactory() {
		super("dbz-json", 1, true);
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
	public DeserializationSchema<ChangeRow> createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = deriveSchema(descriptorProperties.asMap());

		// create and configure
		final JsonRowDeserializationSchema.Builder jsonDeserializerBuilder =
			new JsonRowDeserializationSchema.Builder(createTypeInformation(schema));

		descriptorProperties.getOptionalBoolean(JsonValidator.FORMAT_FAIL_ON_MISSING_FIELD)
			.ifPresent(flag -> {
				if (flag) {
					jsonDeserializerBuilder.failOnMissingField();
				}
			});

		return new DebeziumJsonDeserializationSchema(
			jsonDeserializerBuilder.build(),
			new ChangeRowTypeInfo(schema.toRowType()));
	}

	private RowTypeInfo createTypeInformation(TableSchema schema) {
		TableSchema dbzSchema = TableSchema.builder()
			.field("before", schema.toRowDataType())
			.field("after", schema.toRowDataType())
			.field("source", DataTypes.STRING())
			.field("op", DataTypes.STRING())
			.field("ts_ms", DataTypes.BIGINT())
			.build();
		return (RowTypeInfo) dbzSchema.toRowType();
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new JsonValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
