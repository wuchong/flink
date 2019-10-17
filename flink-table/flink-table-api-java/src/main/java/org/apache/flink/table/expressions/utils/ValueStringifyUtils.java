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

package org.apache.flink.table.expressions.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.flink.table.expressions.utils.ExpressionStringUtils.escapeString;


/**
 * .
 */
@Internal
public class ValueStringifyUtils {

	private static final String SEPARATOR = ", ";
	private static final String NULL_TERM = "null";

	@SuppressWarnings("unchecked")
	public static String stringifyValue(Object value, LogicalType type) {
		if (value == null) {
			return NULL_TERM;
		}
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				// true, false
				return extractValue(value, Boolean.class).toString();
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DECIMAL:
				// all the numeric will be serialized in BigDecimal format
				return extractValue(value, BigDecimal.class).toString();
			case INTERVAL_DAY_TIME:
				// DayTimeIntervalType (Duration/Long) will be serialized in Long format
				return extractValue(value, Long.class).toString();
			case INTERVAL_YEAR_MONTH:
				// YearMonthInterval (Period/Integer) will be serialized in Integer format
				return extractValue(value, Integer.class).toString();
			case DATE:
				// serialized in string representation of LocalDate
				return escapeString(extractValue(value, LocalDate.class).toString());
			case TIME_WITHOUT_TIME_ZONE:
				// serialized in string representation of LocalTime
				return escapeString(extractValue(value, LocalTime.class).toString());
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				// serialized in string representation of LocalDateTime
				return escapeString(extractValue(value, LocalDateTime.class).toString());
			case TIMESTAMP_WITH_TIME_ZONE:
				// maybe OffsetDateTime or ZonedDateTime
				Optional<OffsetDateTime> offsetDateTime = getValueAs(value, OffsetDateTime.class);
				Optional<ZonedDateTime> zonedDateTime = getValueAs(value, ZonedDateTime.class);
				// serialized in either '2007-12-03T10:15:30+01:00' or '2007-12-03T10:15:30+01:00[Europe/Paris]'
				if (offsetDateTime.isPresent()) {
					return escapeString(offsetDateTime.get().toString());
				} else if (zonedDateTime.isPresent()) {
					return escapeString(zonedDateTime.get().toString());
				} else {
					throw new TableException(
						"Literal in TIMESTAMP WITH TIME ZONE type must be OffsetDateTime or ZonedDateTime, " +
							"but is " + value.getClass().getCanonicalName());
				}
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				// serialized in "second.nano" format, e.g. '1570796390.988000000'
				Instant instant = extractValue(value, Instant.class);
				return escapeString(instant.getEpochSecond() + "." + instant.getNano());
			case CHAR:
			case VARCHAR:
				// serialized in String format
				return escapeString(extractValue(value, String.class));
			case MAP: {
				// MAP[key1, value1, key2, value2, ...]
				MapType mapType = (MapType) type;
				LogicalType keyType = mapType.getKeyType();
				LogicalType valueType = mapType.getValueType();
				List<String> entries = new ArrayList<>();
				extractValue(value, Map.class)
					.forEach((k, v) ->
						entries.add(stringifyValue(k, keyType) + SEPARATOR + stringifyValue(v, valueType)));
				return String.format("MAP[%s]", String.join(SEPARATOR, entries));
			}
			case MULTISET: {
				// MULTISET[key1, number, key2, number, ...]
				LogicalType elementType = ((MultisetType) type).getElementType();
				List<String> entries = new ArrayList<>();
				extractValue(value, Map.class)
					.forEach((k, v) ->
						entries.add(stringifyValue(k, elementType) + SEPARATOR + stringifyValue(v, new IntType())));
				return String.format("MULTISET[%s]", String.join(SEPARATOR, entries));
			}
			case ARRAY: {
				// ARRAY[a1, a2, ...]
				LogicalType elementType = ((ArrayType) type).getElementType();
				List<String> elements = new ArrayList<>();
				Stream.of(extractValue(value, Object[].class))
					.forEach(e -> elements.add(stringifyValue(e, elementType)));
				return String.format("ARRAY[%s]", String.join(SEPARATOR, elements));
			}
			case ROW: {
				// ROW(value1, value2, ...)
				RowType rowType = (RowType) type;
				Row row = extractValue(value, Row.class);
				List<String> fields = new ArrayList<>();
				for (int i = 0; i < rowType.getFieldCount(); i++) {
					fields.add(stringifyValue(row.getField(i), rowType.getTypeAt(i)));
				}
				return String.format("ROW(%s)", String.join(SEPARATOR, fields));
			}
			case BINARY:
			case VARBINARY:
				// BINARY and VARBINARY are serialized in base64 format
				byte[] binary = extractValue(value, byte[].class);
				return escapeString(EncodingUtils.encodeBytesToBase64(binary));
			case ANY:
				AnyType anyType = (AnyType) type;
				Object object = extractValue(value, Object.class);
				try {
					// serialize to byte[] and store in base64 format
					byte[] bytes = InstantiationUtil.serializeToByteArray(anyType.getTypeSerializer(), object);
					return escapeString(EncodingUtils.encodeBytesToBase64(bytes));
				} catch (IOException e) {
					throw new ValidationException("Failed to serialize value as type: " + anyType, e);
				}

			case SYMBOL:
				// values of symbol type are enum in Java, use the enum string format
				return extractValue(value, Object.class).toString();
			default:
				throw new UnsupportedOperationException(
					"Literal of " + type.getTypeRoot() + " type is not string serializable for now.");
		}
	}

	/**
	 * Returns the value (excluding null) as an instance of the given class.
	 */
	@SuppressWarnings("unchecked")
	private static <T> Optional<T> getValueAs(Object value, Class<T> clazz) {
		if (value == null) {
			return Optional.empty();
		}

		final Class<?> valueClass = value.getClass();

		Object convertedValue = null;

		if (clazz.isInstance(value)) {
			convertedValue = clazz.cast(value);
		}

		else if (valueClass == Integer.class && clazz == Long.class) {
			final Integer integer = (Integer) value;
			convertedValue = integer.longValue();
		}

		else if (valueClass == Duration.class && clazz == Long.class) {
			final Duration duration = (Duration) value;
			convertedValue = duration.toMillis();
		}

		else if (valueClass == Period.class && clazz == Integer.class) {
			final Period period = (Period) value;
			convertedValue = period.toTotalMonths();
		}

		else if (valueClass == java.sql.Date.class && clazz == java.time.LocalDate.class) {
			final java.sql.Date date = (java.sql.Date) value;
			convertedValue = date.toLocalDate();
		}

		else if (valueClass == java.sql.Time.class && clazz == java.time.LocalTime.class) {
			final java.sql.Time time = (java.sql.Time) value;
			convertedValue = time.toLocalTime();
		}

		else if (valueClass == java.sql.Timestamp.class && clazz == java.time.LocalDateTime.class) {
			final java.sql.Timestamp timestamp = (java.sql.Timestamp) value;
			convertedValue = timestamp.toLocalDateTime();
		}

		else if (Number.class.isAssignableFrom(valueClass) && clazz == BigDecimal.class) {
			convertedValue = new BigDecimal(String.valueOf(value));
		}

		else if (valueClass == byte[].class && clazz == String.class) {
			convertedValue = new String((byte[]) value);
		}

		// we can offer more conversions in the future, these conversions must not necessarily
		// comply with the logical type conversions
		return Optional.ofNullable((T) convertedValue);
	}

	private static <T> T extractValue(Object value, Class<T> clazz) {
		return getValueAs(value, clazz)
			.orElseThrow(() -> new TableException("Unsupported class " + clazz + " for value " + value.getClass()));
	}
}
