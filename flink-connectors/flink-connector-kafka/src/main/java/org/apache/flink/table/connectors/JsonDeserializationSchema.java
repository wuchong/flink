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

package org.apache.flink.table.connectors;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.connectors.SupportsChangelogReading.Context;
import org.apache.flink.table.connectors.SupportsChangelogReading.DataFormatConverter;
import org.apache.flink.table.connectors.SupportsChangelogReading.RowFormatProducer;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonDeserializationSchema implements DeserializationSchema<ChangelogRow> {

	private final ObjectMapper objectMapper = new ObjectMapper();

	private final JsonNodeFormatConverter runtimeConverter;

	private JsonDeserializationSchema(JsonNodeFormatConverter runtimeConverter) {
		this.runtimeConverter = runtimeConverter;
	}

	/**
	 * Creates a JSON schema.
	 */
	public static JsonDeserializationSchema create(DataType dataType, Context context) {
		final JsonNodeConverterGenerator generator = new JsonNodeConverterGenerator(context);
		return new JsonDeserializationSchema(dataType.accept(generator));
	}

	// TODO not existing yet
	public void open() {
		runtimeConverter.open(FormatConverter.Context.empty());
	}

	@Override
	public ChangelogRow deserialize(byte[] message) throws IOException {
		final JsonNode root = objectMapper.readTree(message);
		return (ChangelogRow) runtimeConverter.convert(root);
	}

	@Override
	public boolean isEndOfStream(ChangelogRow nextElement) {
		return false;
	}

	@Override
	public TypeInformation<ChangelogRow> getProducedType() {
		return null;
	}

	// ---------------------------------------------------------------------------------------------

	private static class JsonNodeConverterGenerator implements DataTypeVisitor<JsonNodeFormatConverter> {

		private final Context context;

		public JsonNodeConverterGenerator(Context context) {
			this.context = context;
		}

		@Override
		public JsonNodeFormatConverter visit(AtomicDataType atomicDataType) {
			final LogicalType type = atomicDataType.getLogicalType();
			final DataFormatConverter converter = context.createDataFormatConverter(atomicDataType);
			switch (type.getTypeRoot()) {

				case VARCHAR:
					return new JsonNodeFormatConverter(converter) {
						@Override
						public Object convert(JsonNode jsonNode) {
							return converter.toInternal(jsonNode.asText());
						}
					};

				case BOOLEAN:
					return new JsonNodeFormatConverter(converter) {
						@Override
						public Object convert(JsonNode jsonNode) {
							return converter.toInternal(jsonNode.asBoolean());
						}
					};

				default:
					throw new UnsupportedOperationException();
			}
		}

		@Override
		public JsonNodeFormatConverter visit(CollectionDataType collectionDataType) {
			throw new UnsupportedOperationException();
		}

		@Override
		public JsonNodeFormatConverter visit(FieldsDataType fieldsDataType) {
			final LogicalType type = fieldsDataType.getLogicalType();
			switch (type.getTypeRoot()) {

				case ROW:
					final RowFormatProducer producer = context.createRowFormatProducer(fieldsDataType);

					final List<DataType> fields = fieldsDataType.getFieldDataTypesNew();
					final List<String> names = fieldsDataType.getFieldNames();
					final JsonNodeFormatConverter[] fieldConverters = new JsonNodeFormatConverter[fields.size()];
					for (int i = 0; i < fields.size(); i++) {
						fieldConverters[i] = fields.get(i).accept(this);
					}

					return new JsonNodeFormatConverter(producer, fieldConverters) {
						@Override
						public Object convert(JsonNode jsonNode) {
							ObjectNode node = (ObjectNode) jsonNode;
							for (int i = 0; i < names.size(); i++) {
								JsonNode field = node.get(names.get(i));
								producer.setField(i, fieldConverters[i].convert(field));
							}
							return producer.toInternal();
						}
					};

				default:
					throw new UnsupportedOperationException();
			}
		}

		@Override
		public JsonNodeFormatConverter visit(KeyValueDataType keyValueDataType) {
			throw new UnsupportedOperationException();
		}
	}

	private static abstract class JsonNodeFormatConverter implements FormatConverter, Serializable {

		private final List<FormatConverter> formatConverters;

		public JsonNodeFormatConverter(
				FormatConverter containerConverter,
				FormatConverter... fieldConverters) {
			this.formatConverters = Stream.concat(
					Stream.of(containerConverter),
					Stream.of(fieldConverters))
				.collect(Collectors.toList());
		}

		@Override
		public void open(Context context) {
			formatConverters.forEach(c -> c.open(context));
		}

		abstract Object convert(JsonNode jsonNode);
	}
}
