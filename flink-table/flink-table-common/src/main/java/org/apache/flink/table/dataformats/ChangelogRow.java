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

import java.io.Serializable;

public class ChangelogRow implements TypedGetterSetters, Serializable {

	private static final long serialVersionUID = 1L;

	private ChangelogKind kind;
	private BaseRow row;

	/**
	 * Get the number of fields in the BaseRow.
	 *
	 * @return The number of fields in the BaseRow.
	 */
	public int getArity() {
		return row.getArity();
	}

	/**
	 * The header represents the type of this Row. Now just used in streaming.
	 * Now there are two message: ACCUMULATE_MSG and RETRACT_MSG.
	 */
	public ChangelogKind getChangelogKind() {
		return kind;
	}

	/**
	 * Set the byte header.
	 */
	public void setChangelogKind(ChangelogKind kind) {
		this.kind = kind;
	}

	// for object reuse
	public void replace(BaseRow row) {
		this.row = row;
	}

	public BaseRow toBaseRow() {
		return row;
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return row.isNullAt(ordinal);
	}

	@Override
	public void setNullAt(int ordinal) {
		row.setNullAt(ordinal);
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return row.getBoolean(ordinal);
	}

	@Override
	public byte getByte(int ordinal) {
		return row.getByte(ordinal);
	}

	@Override
	public short getShort(int ordinal) {
		return row.getShort(ordinal);
	}

	@Override
	public int getInt(int ordinal) {
		return row.getInt(ordinal);
	}

	@Override
	public long getLong(int ordinal) {
		return row.getLong(ordinal);
	}

	@Override
	public float getFloat(int ordinal) {
		return row.getFloat(ordinal);
	}

	@Override
	public double getDouble(int ordinal) {
		return row.getDouble(ordinal);
	}

	@Override
	public BinaryString getString(int ordinal) {
		return row.getString(ordinal);
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return row.getDecimal(ordinal, precision, scale);
	}

	@Override
	public SqlTimestamp getTimestamp(int ordinal, int precision) {
		return row.getTimestamp(ordinal, precision);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int ordinal) {
		return row.getGeneric(ordinal);
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return row.getBinary(ordinal);
	}

	@Override
	public BaseArray getArray(int ordinal) {
		return row.getArray(ordinal);
	}

	@Override
	public BaseMap getMap(int ordinal) {
		return row.getMap(ordinal);
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		return row.getRow(ordinal, numFields);
	}

	@Override
	public void setBoolean(int ordinal, boolean value) {
		row.setBoolean(ordinal, value);
	}

	@Override
	public void setByte(int ordinal, byte value) {
		row.setByte(ordinal, value);
	}

	@Override
	public void setShort(int ordinal, short value) {
		row.setShort(ordinal, value);
	}

	@Override
	public void setInt(int ordinal, int value) {
		row.setInt(ordinal, value);
	}

	@Override
	public void setLong(int ordinal, long value) {
		row.setLong(ordinal, value);
	}

	@Override
	public void setFloat(int ordinal, float value) {
		row.setFloat(ordinal, value);
	}

	@Override
	public void setDouble(int ordinal, double value) {
		row.setDouble(ordinal, value);
	}

	@Override
	public void setDecimal(int ordinal, Decimal value, int precision) {
		row.setDecimal(ordinal, value, precision);
	}

	@Override
	public void setTimestamp(int ordinal, SqlTimestamp value, int precision) {
		row.setTimestamp(ordinal, value, precision);
	}
}
