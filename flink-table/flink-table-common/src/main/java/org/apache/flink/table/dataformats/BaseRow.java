/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformats;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * An interface for row used internally in Flink Table/SQL, which only contains the columns as
 * internal types.
 *
 * <p>A {@link BaseRow} also contains {@link ChangelogKind} which is a metadata information of
 * this row, not a column of this row. The {@link ChangelogKind} represents the changelog operation
 * kind: INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER.
 *
 * <p>TODO: add a list and description for all internal formats
 *
 * <p>There are different implementations depending on the scenario. For example, the binary-orient
 * implementation {@link BinaryRow} and the object-orient implementation {@link GenericRow}. All
 * the different implementations have the same binary format after serialization.
 *
 * <p>{@code BaseRow}s are influenced by Apache Spark InternalRows.
 */
@PublicEvolving
public interface BaseRow extends Serializable {

	/**
	 * Get the number of fields in the BaseRow.
	 *
	 * @return The number of fields in the BaseRow.
	 */
	int getArity();

	/**
	 * Gets the changelog kind of this row, it is a metadata of this row, not a column of this row.
	 */
	ChangelogKind getChangelogKind();

	/**
	 * Sets the changelog kind of this row, it is a metadata of this row, not a column of this row.
	 */
	void setChangelogKind(ChangelogKind kind);

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
}
