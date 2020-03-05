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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connectors.ChangelogDeserializationSchema;
import org.apache.flink.table.connectors.ChangelogMode;
import org.apache.flink.table.dataformats.BaseRow;
import org.apache.flink.table.dataformats.Decimal;
import org.apache.flink.table.dataformats.GenericArray;
import org.apache.flink.table.dataformats.GenericMap;
import org.apache.flink.table.dataformats.GenericRow;
import org.apache.flink.table.dataformats.SqlTimestamp;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIME_FORMAT;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data formats.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 */
@Internal
public class JsonBaseRowDeserializationSchema implements ChangelogDeserializationSchema {
	private static final long serialVersionUID = 8576854315236033439L;

	/** logical type describing the result type. **/
	private final RowType rowType;

	private final DeserializationRuntimeConverter runtimeConverter;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	public JsonBaseRowDeserializationSchema(RowType rowType) {
		this.rowType = rowType;
		this.runtimeConverter = createConverter(rowType);
	}

	@Override
	public BaseRow deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return (GenericRow) runtimeConverter.convert(root);
		} catch (Throwable t) {
			throw new IOException("Failed to deserialize JSON object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(BaseRow nextElement) {
		return false;
	}

	@Override
	public TypeInformation<BaseRow> getProducedType() {
		return new BaseRowTypeInfo(rowType);
	}

	@Override
	public ChangelogMode producedChangelogMode() {
//		ChangelogMode.newBuilder().addSupportedKind(ChangelogKind.INSERT).build();
		return ChangelogMode.insertOnly();
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that maps between {@link JsonNode}s and Java objects.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(JsonNode jsonNode);
	}

	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return jsonNode -> null;
			case BOOLEAN:
				return JsonNode::asBoolean;
			case TINYINT:
				return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
			case SMALLINT:
				return jsonNode -> Short.parseShort(jsonNode.asText().trim());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return JsonNode::asInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return JsonNode::asLong;
			case DATE:
				return this::convertToInternalDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToInternalTime;
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToInternalTimestamp;
			case FLOAT:
				return jsonNode -> Float.parseFloat(jsonNode.asText().trim());
			case DOUBLE:
				return JsonNode::asDouble;
			case CHAR:
			case VARCHAR:
				return JsonNode::asText;
			case DECIMAL:
				return jsonNode -> convertToInternalDecimal(jsonNode, (DecimalType) type);
			case BINARY:
			case VARBINARY:
				return this::convertToBytes;
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

	private int convertToInternalDate(JsonNode jsonNode) {
		LocalDate date = ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToInternalTime(JsonNode jsonNode) {
		// according to RFC 3339 every full-time must have a timezone;
		// until we have full timezone support, we only support UTC;
		// users can parse their time as string as a workaround
		TemporalAccessor parsedTime = RFC3339_TIME_FORMAT.parse(jsonNode.asText());

		ZoneOffset zoneOffset = parsedTime.query(TemporalQueries.offset());
		LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

		if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0 || localTime.getNano() != 0) {
			throw new IllegalStateException(
				"Invalid time format. Only a time in UTC timezone without milliseconds is supported yet.");
		}

		// planner only support TIME(0) for now.
		return localTime.toSecondOfDay();
	}

	private SqlTimestamp convertToInternalTimestamp(JsonNode jsonNode) {
		// according to RFC 3339 every date-time must have a timezone;
		// until we have full timezone support, we only support UTC;
		// users can parse their time as string as a workaround
		TemporalAccessor parsedTimestamp = RFC3339_TIMESTAMP_FORMAT.parse(jsonNode.asText());

		ZoneOffset zoneOffset = parsedTimestamp.query(TemporalQueries.offset());

		if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0) {
			throw new IllegalStateException(
				"Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. " +
					"Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		}

		LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

		return SqlTimestamp.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
	}

	private Decimal convertToInternalDecimal(JsonNode jsonNode, DecimalType decimalType) {
		BigDecimal bigDecimal = jsonNode.decimalValue();
		return Decimal.fromBigDecimal(bigDecimal, decimalType.getPrecision(), decimalType.getScale());
	}

	private byte[] convertToBytes(JsonNode jsonNode) {
		try {
			return jsonNode.binaryValue();
		} catch (IOException e) {
			throw new WrappingRuntimeException("Unable to deserialize byte array.", e);
		}
	}


	private DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		DeserializationRuntimeConverter elementConverter = createConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.internalConversionClass(arrayType.getElementType());
		return jsonNode -> {
			final ArrayNode node = (ArrayNode) jsonNode;
			final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
			for (int i = 0; i < node.size(); i++) {
				final JsonNode innerNode = node.get(i);
				array[i] = elementConverter.convert(innerNode);
			}
			return new GenericArray(array);
		};
	}

	private DeserializationRuntimeConverter createMapConverter(MapType mapType) {
		DeserializationRuntimeConverter keyConverter = createConverter(mapType.getKeyType());
		DeserializationRuntimeConverter valueConverter = createConverter(mapType.getValueType());

		return jsonNode -> {
			Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
			Map<Object, Object> result = new HashMap<>();
			while (fields.hasNext()) {
				Map.Entry<String, JsonNode> entry = fields.next();
				Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
				Object value = valueConverter.convert(entry.getValue());
				result.put(key, value);
			}
			return new GenericMap(result);
		};
	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
		return jsonNode -> {
			ObjectNode node = (ObjectNode) jsonNode;
			int arity = fieldNames.length;
			GenericRow row = new GenericRow(arity);
			for (int i = 0; i < arity; i++) {
				JsonNode field = node.get(fieldNames[i]);
				row.setField(i, fieldConverters[i].convert(field));
			}
			return row;
		};
	}
}
