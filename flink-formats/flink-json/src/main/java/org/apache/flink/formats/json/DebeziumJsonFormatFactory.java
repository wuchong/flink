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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogSerializationSchema;
import org.apache.flink.table.dataformats.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.JsonValidator;
import org.apache.flink.table.factories.ChangelogDeserializationSchemaFactory;
import org.apache.flink.table.factories.ChangelogSerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DebeziumJsonFormatFactory extends TableFormatFactoryBase<RowData>
		implements ChangelogDeserializationSchemaFactory, ChangelogSerializationSchemaFactory {

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
	public ChangelogDeserializationSchema createDeserializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = deriveSchema(descriptorProperties.asMap());
		return new DebeziumJsonDeserializationSchema(schema);
	}

	@Override
	public ChangelogSerializationSchema createSerializationSchema(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = deriveSchema(descriptorProperties.asMap());
		return new DebeziumJsonSerializationSchema(schema);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(propertiesMap);

		// validate
		new JsonValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}