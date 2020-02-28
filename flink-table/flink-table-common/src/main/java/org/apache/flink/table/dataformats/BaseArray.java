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

import java.io.Serializable;

/**
 * An interface for array used internally in Flink Table/SQL.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryArray} format.
 * Convenient updates use the {@link GenericArray} format.
 */
@PublicEvolving
public interface BaseArray extends Serializable {

	int numElements();

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
	BinaryString getString(int ordinal);

	/**
	 * Get decimal value, internal format is Decimal.
	 */
	Decimal getDecimal(int ordinal, int precision, int scale);

	/**
	 * Get Timestamp value, internal format is SqlTimestamp.
	 */
	SqlTimestamp getTimestamp(int ordinal, int precision);

	/**
	 * Get generic value, internal format is BinaryGeneric.
	 */
	<T> BinaryGeneric<T> getGeneric(int ordinal);

	/**
	 * Get binary value, internal format is byte[].
	 */
	byte[] getBinary(int ordinal);

	/**
	 * Get array value, internal format is BaseArray.
	 */
	BaseArray getArray(int ordinal);

	/**
	 * Get map value, internal format is BaseMap.
	 */
	BaseMap getMap(int ordinal);

	/**
	 * Get row value, internal format is BaseRow.
	 */
	BaseRow getRow(int ordinal, int numFields);

	boolean[] toBooleanArray();

	byte[] toByteArray();

	short[] toShortArray();

	int[] toIntArray();

	long[] toLongArray();

	float[] toFloatArray();

	double[] toDoubleArray();

	// ------------------------------------------------------------------------------------------

	static Object get(BaseArray array, int ordinal, LogicalType type) {
		switch (type.getTypeRoot()) {
			case BOOLEAN:
				return array.getBoolean(ordinal);
			case TINYINT:
				return array.getByte(ordinal);
			case SMALLINT:
				return array.getShort(ordinal);
			case INTEGER:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
				return array.getInt(ordinal);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return array.getLong(ordinal);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				TimestampType timestampType = (TimestampType) type;
				return array.getTimestamp(ordinal, timestampType.getPrecision());
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				LocalZonedTimestampType lzTs = (LocalZonedTimestampType) type;
				return array.getTimestamp(ordinal, lzTs.getPrecision());
			case FLOAT:
				return array.getFloat(ordinal);
			case DOUBLE:
				return array.getDouble(ordinal);
			case CHAR:
			case VARCHAR:
				return array.getString(ordinal);
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				return array.getDecimal(ordinal, decimalType.getPrecision(), decimalType.getScale());
			case ARRAY:
				return array.getArray(ordinal);
			case MAP:
			case MULTISET:
				return array.getMap(ordinal);
			case ROW:
				return array.getRow(ordinal, ((RowType) type).getFieldCount());
			case BINARY:
			case VARBINARY:
				return array.getBinary(ordinal);
			case RAW:
				return array.getGeneric(ordinal);
			default:
				throw new RuntimeException("Not support type: " + type);
		}
	}
}
