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

import org.apache.flink.annotation.Internal;

/**
 * Provide type specialized setters to reduce if/else and eliminate box and unbox. This is mainly
 * used on the binary format such as {@link BinaryRow}.
 */
@Internal
public interface TypedSetters {

	/**
	 * Set null to this field.
	 */
	void setNullAt(int ordinal);

	/**
	 * Set boolean value.
	 */
	void setBoolean(int ordinal, boolean value);

	/**
	 * Set byte value.
	 */
	void setByte(int ordinal, byte value);

	/**
	 * Set short value.
	 */
	void setShort(int ordinal, short value);

	/**
	 * Set int value.
	 */
	void setInt(int ordinal, int value);

	/**
	 * Set long value.
	 */
	void setLong(int ordinal, long value);

	/**
	 * Set float value.
	 */
	void setFloat(int ordinal, float value);

	/**
	 * Set double value.
	 */
	void setDouble(int ordinal, double value);

	/**
	 * Set the decimal column value.
	 *
	 * <p>Note:
	 * Precision is compact: can call setNullAt when decimal is null.
	 * Precision is not compact: can not call setNullAt when decimal is null, must call
	 * setDecimal(i, null, precision) because we need update var-length-part.
	 */
	void setDecimal(int i, SqlDecimal value, int precision);

	/**
	 * Set Timestamp value.
	 *
	 * <p>Note:
	 * If precision is compact: can call setNullAt when SqlTimestamp value is null.
	 * Otherwise: can not call setNullAt when SqlTimestamp value is null, must call
	 * setTimestamp(ordinal, null, precision) because we need to update var-length-part.
	 */
	void setTimestamp(int ordinal, SqlTimestamp value, int precision);
}
