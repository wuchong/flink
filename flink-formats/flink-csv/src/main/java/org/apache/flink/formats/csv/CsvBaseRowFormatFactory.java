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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.descriptors.CsvValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.ChangelogDeserializationSchemaFactory;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Table format factory for providing configured instances of CSV-to-row {@link SerializationSchema}
 * and {@link DeserializationSchema}.
 */
public final class CsvBaseRowFormatFactory extends TableFormatFactoryBase<BaseRow>
	implements ChangelogDeserializationSchemaFactory {

	public CsvBaseRowFormatFactory() {
		super(CsvValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	public List<String> supportedFormatProperties() {
		final List<String> properties = new ArrayList<>();
		properties.add(CsvValidator.FORMAT_FIELD_DELIMITER);
		properties.add(CsvValidator.FORMAT_LINE_DELIMITER);
		properties.add(CsvValidator.FORMAT_DISABLE_QUOTE_CHARACTER);
		properties.add(CsvValidator.FORMAT_QUOTE_CHARACTER);
		properties.add(CsvValidator.FORMAT_ALLOW_COMMENTS);
		properties.add(CsvValidator.FORMAT_IGNORE_PARSE_ERRORS);
		properties.add(CsvValidator.FORMAT_ARRAY_ELEMENT_DELIMITER);
		properties.add(CsvValidator.FORMAT_ESCAPE_CHARACTER);
		properties.add(CsvValidator.FORMAT_NULL_LITERAL);
		properties.add(CsvValidator.FORMAT_SCHEMA);
		return properties;
	}

	@Override
	public ChangelogDeserializationSchema createDeserializationSchema(Map<String, String> properties) {
		DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		TableSchema schema = deriveSchema(descriptorProperties.asMap());
		RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
		String fieldDelimiter = descriptorProperties.getOptionalString(CsvValidator.FORMAT_FIELD_DELIMITER)
			.orElse(CsvBaseRowDeserializationSchema.DEFAULT_FIELD_DELIMITER);
		return new CsvBaseRowDeserializationSchema(fieldDelimiter, rowType);
	}

	private static DescriptorProperties getValidatedProperties(Map<String, String> propertiesMap) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(propertiesMap);

		new CsvValidator().validate(descriptorProperties);

		return descriptorProperties;
	}
}
