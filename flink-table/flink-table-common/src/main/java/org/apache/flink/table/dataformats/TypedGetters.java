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

package org.apache.flink.table.dataformats;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * Provide type specialized getters to reduce if/else and eliminate box and unbox. This is mainly
 * used on the binary format such as {@link BinaryRowData}.
 */
@PublicEvolving
public interface TypedGetters {

	/**
	 * Because the specific row implementation such as BinaryRow uses the binary format. We must
	 * first determine if it is null, and then make a specific get.
	 *
	 * @return true if this field is null.
	 */
	boolean isNullAt(int ordinal);

	/**
	 * Get boolean value.
	 */
	boolean getBoolean(int ordinal);

	/**
	 * Get byte value.
	 */
	byte getByte(int ordinal);

	/**
	 * Get short value.
	 */
	short getShort(int ordinal);

	/**
	 * Get int value.
	 */
	int getInt(int ordinal);

	/**
	 * Get long value.
	 */
	long getLong(int ordinal);

	/**
	 * Get float value.
	 */
	float getFloat(int ordinal);

	/**
	 * Get double value.
	 */
	double getDouble(int ordinal);

	/**
	 * Get string value, internal format is BinaryString.
	 */
	StringData getString(int ordinal);

	/**
	 * Get decimal value, internal format is Decimal.
	 */
	DecimalData getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get Timestamp value, internal format is SqlTimestamp.
	 */
	TimestampData getTimestamp(int ordinal, int precision);

	/**
	 * Get generic value, internal format is BinaryGeneric.
	 */
	<T> RawValueData<T> getGeneric(int ordinal);

	/**
	 * Get binary value, internal format is byte[].
	 */
	byte[] getBinary(int ordinal);

	/**
	 * Get array value, internal format is BaseArray.
	 */
	ArrayData getArray(int ordinal);

	/**
	 * Get map value, internal format is BaseMap.
	 */
	MapData getMap(int ordinal);

	/**
	 * Get row value, internal format is BaseRow.
	 */
	RowData getRow(int ordinal, int numFields);

	// ------------------------------------------------------------------------------------------

	static Object get(TypedGetters object, int ordinal, LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return object.getBoolean(ordinal);
			case TINYINT:
				return object.getByte(ordinal);
			case SMALLINT:
				return object.getShort(ordinal);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return object.getInt(ordinal);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return object.getLong(ordinal);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) type;
				return object.getTimestamp(ordinal, timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				return object.getTimestamp(ordinal, lzTs.getPrecision());
			case FLOAT:
				return object.getFloat(ordinal);
			case DOUBLE:
				return object.getDouble(ordinal);
			case CHAR:
			case VARCHAR:
				return object.getString(ordinal);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return object.getDecimal(ordinal, decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return object.getArray(ordinal);
			case MAP:
			case MULTISET:
				return object.getMap(ordinal);
			case ROW:
				return object.getRow(ordinal, ((RowType) type).getFieldCount());
			case BINARY:
			case VARBINARY:
				return object.getBinary(ordinal);
			case RAW:
				return object.getGeneric(ordinal);
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
