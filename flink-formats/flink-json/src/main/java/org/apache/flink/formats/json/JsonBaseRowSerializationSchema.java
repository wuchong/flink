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
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.connectors.ChangelogSerializationSchema;
import org.apache.flink.table.dataformats.BaseArray;
import org.apache.flink.table.dataformats.BaseMap;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.dataformats.Decimal;
import org.apache.flink.table.dataformats.SqlTimestamp;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIME_FORMAT;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Serialization schema that serializes an object of Flink internal data formats into a JSON bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link JsonBaseRowDeserializationSchema}.
 */
@PublicEvolving
public class JsonBaseRowSerializationSchema implements ChangelogSerializationSchema {
	private static final long serialVersionUID = 1L;

	/** Logical type describing the input type. */
	private final RowType rowType;
	/** The converter that converts internal data formats to JsonNode. */
	private final SerializationRuntimeConverter runtimeConverter;
	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	/** Reusable object node. */
	private transient ObjectNode node;

	public JsonBaseRowSerializationSchema(RowType rowType) {
		this.rowType = rowType;
		this.runtimeConverter = createConverter(rowType);
	}

	@Override
	public ChangelogMode supportedChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public byte[] serialize(BaseRow row) {
		if (node == null) {
			node = mapper.createObjectNode();
		}

		try {
			runtimeConverter.convert(mapper, node, row);
			return mapper.writeValueAsBytes(node);
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'. " +
				"Make sure that the schema matches the input.", t);
		}
	}

	// --------------------------------------------------------------------------------
	// Runtime Converters
	// --------------------------------------------------------------------------------

	private interface SerializationRuntimeConverter extends Serializable {
		JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
	}

	private SerializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
			case BOOLEAN:
				return (mapper, reuse, value) -> mapper.getNodeFactory().booleanNode((boolean) value);
			case TINYINT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
			case SMALLINT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
			case DATE:
				return createDateConverter();
			case TIME_WITHOUT_TIME_ZONE:
				return createTimeConverter();
			case TIMESTAMP_WITH_TIME_ZONE:
				return createTimestampConverter(((ZonedTimestampType) type).getPrecision());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return createTimestampConverter(((TimestampType) type).getPrecision());
			case FLOAT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
			case DOUBLE:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
			case CHAR:
			case VARCHAR:
				// value is BinaryString
				return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			case BINARY:
			case VARBINARY:
				return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case MAP:
			case MULTISET:
				return createMapConverter((MapType) type);
			case ROW:
				return createRowConverter((RowType) type);
			case RAW:
			default:
				throw new UnsupportedOperationException("Not support to parse type: " + type);
		}
	}

	private SerializationRuntimeConverter createDecimalConverter(DecimalType type) {
		int precision = type.getPrecision();
		int scale = type.getScale();
		return (mapper, reuse, value) -> {
			BigDecimal bd = ((Decimal) value).toBigDecimal();
			return mapper.getNodeFactory().numberNode(bd);
		};
	}

	private SerializationRuntimeConverter createDateConverter() {
		return (mapper, reuse, value) -> {
			int days = (int) value;
			LocalDate date = LocalDate.ofEpochDay(days);
			return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
		};
	}

	private SerializationRuntimeConverter createTimeConverter() {
		return (mapper, reuse, value) -> {
			int seconds = (int) value;
			LocalTime time = LocalTime.ofSecondOfDay(seconds);
			return mapper.getNodeFactory().textNode(RFC3339_TIME_FORMAT.format(time));
		};
	}

	private SerializationRuntimeConverter createTimestampConverter(int precision) {
		return (mapper, reuse, value) -> {
			SqlTimestamp timestamp = (SqlTimestamp) value;
			return mapper.getNodeFactory()
				.textNode(RFC3339_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
		};
	}

	private SerializationRuntimeConverter createArrayConverter(ArrayType type) {
		final LogicalType elementType = type.getElementType();
		final SerializationRuntimeConverter elementConverter = createConverter(elementType);
		return (mapper, reuse, value) -> {
			ArrayNode node;

			if (reuse == null) {
				node = mapper.createArrayNode();
			} else {
				node = (ArrayNode) reuse;
				node.removeAll();
			}

			BaseArray array = (BaseArray) value;
			int numElements = array.numElements();
			for (int i = 0; i < numElements; i++) {
				Object element = BaseArray.get(array, i, elementType);
				node.add(elementConverter.convert(mapper, null, element));
			}

			return node;
		};
	}

	private SerializationRuntimeConverter createMapConverter(MapType type) {
		checkArgument(LogicalTypeChecks.hasFamily(type.getKeyType(), LogicalTypeFamily.CHARACTER_STRING));
		final LogicalType valueType = type.getValueType();
		final SerializationRuntimeConverter valueConverter = createConverter(valueType);
		return (mapper, reuse, object) -> {
			ObjectNode node;
			if (reuse == null) {
				node = mapper.createObjectNode();
			} else {
				node = (ObjectNode) reuse;
			}

			BaseMap map = (BaseMap) object;
			BaseArray keyArray = map.keyArray();
			BaseArray valueArray = map.valueArray();
			int numElements = map.numElements();

			for (int i = 0; i < numElements; i++) {
				String fieldName = keyArray.getString(i).toString(); // key must be string
				Object value = BaseArray.get(valueArray, i, valueType);
				node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
			}

			return node;
		};
	}

	private SerializationRuntimeConverter createRowConverter(RowType type) {
		final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
		final LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final SerializationRuntimeConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(this::createConverter)
			.toArray(SerializationRuntimeConverter[]::new);
		final int fieldCount = type.getFieldCount();

		return (mapper, reuse, value) -> {
			ObjectNode node;
			if (reuse == null) {
				node = mapper.createObjectNode();
			} else {
				node = (ObjectNode) reuse;
			}
			BaseRow row = (BaseRow) value;
			for (int i = 0; i < fieldCount; i++) {
				String fieldName = fieldNames[i];
				Object field = BaseRow.get(row, i, fieldTypes[i]);
				node.set(fieldName, fieldConverters[i].convert(mapper, node.get(fieldName), field));
			}
			return node;
		};
	}
}
