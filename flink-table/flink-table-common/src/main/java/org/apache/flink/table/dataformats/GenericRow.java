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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A GenericRow can have arbitrary number of fields and contain a set of fields, which may all be
 * different types. The fields in GenericRow can be null.
 *
 * <p>The fields in the Row can be accessed by position (zero-based) {@link #getInt}.
 * And can update fields by {@link #setField(int, Object)}.
 *
 * <p>GenericRow is Java serializable, and all internal data formats are Java serializable. However,
 * in case of it contains a non-serializable generic fields, the Java serialization will fail.
 */
@PublicEvolving
public final class GenericRow implements BaseRow {

	private static final long serialVersionUID = 1L;

	/** The array to store the actual internal format values. */
	private final Object[] fields;
	/** The changelog kind of this row. */
	private ChangelogKind kind;

	/**
	 * Create a new GenericRow instance.
	 * @param arity The number of fields in the GenericRow
	 */
	public GenericRow(int arity) {
		this.fields = new Object[arity];
		this.kind = ChangelogKind.INSERT; // INSERT as default
	}

	/**
	 * Sets the field at the specified ordinal.
	 *
	 * <p>Note: the given field value must in internal format, otherwise the {@link GenericRow} is
	 * corrupted, and may throw exception when processing. See the description of {@link BaseRow}
	 * for more information about internal format.
	 *
	 * @param ordinal The ordinal of the field, 0-based.
	 * @param value The internal format value to be assigned to the field at the specified ordinal.
	 * @throws IndexOutOfBoundsException Thrown, if the ordinal is negative, or equal to, or larger than the number of fields.
	 */
	public void setField(int ordinal, Object value) {
		this.fields[ordinal] = value;
	}

	/**
	 * Gets the field at the specified ordinal.
	 *
	 * <p>Note: the returned value is in internal format. See the description of {@link BaseRow}
	 * for more information about internal format.
	 *
	 * @param ordinal The ordinal of the field, 0-based.
	 * @return The field at the specified position.
	 * @throws IndexOutOfBoundsException Thrown, if the ordinal is negative, or equal to, or larger than the number of fields.
	 */
	public Object getField(int ordinal) {
		return this.fields[ordinal];
	}

	@Override
	public int getArity() {
		return fields.length;
	}

	@Override
	public ChangelogKind getChangelogKind() {
		return kind;
	}

	@Override
	public void setChangelogKind(ChangelogKind kind) {
		checkNotNull(kind);
		this.kind = kind;
	}

	@Override
	public boolean isNullAt(int ordinal) {
		return this.fields[ordinal] == null;
	}

	@Override
	public boolean getBoolean(int ordinal) {
		return (boolean) this.fields[ordinal];
	}

	@Override
	public byte getByte(int ordinal) {
		return (byte) this.fields[ordinal];
	}

	@Override
	public short getShort(int ordinal) {
		return (short) this.fields[ordinal];
	}

	@Override
	public int getInt(int ordinal) {
		return (int) this.fields[ordinal];
	}

	@Override
	public long getLong(int ordinal) {
		return (long) this.fields[ordinal];
	}

	@Override
	public float getFloat(int ordinal) {
		return (float) this.fields[ordinal];
	}

	@Override
	public double getDouble(int ordinal) {
		return (double) this.fields[ordinal];
	}

	@Override
	public SqlString getString(int ordinal) {
		return (SqlString) this.fields[ordinal];
	}

	@Override
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return (Decimal) this.fields[ordinal];
	}

	@Override
	public SqlTimestamp getTimestamp(int ordinal, int precision) {
		return (SqlTimestamp) this.fields[ordinal];
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> SqlRawValue<T> getGeneric(int ordinal) {
		return (SqlRawValue<T>) this.fields[ordinal];
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return new byte[0];
	}

	@Override
	public BaseArray getArray(int ordinal) {
		return (BaseArray) this.fields[ordinal];
	}

	@Override
	public BaseMap getMap(int ordinal) {
		return (BaseMap) this.fields[ordinal];
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		return (BaseRow) this.fields[ordinal];
	}

	// ----------------------------------------------------------------------------------------
	// Utilities
	// ----------------------------------------------------------------------------------------

	/**
	 * Creates a GenericRow with the given internal format values and a default
	 * {@link ChangelogKind#INSERT}.
	 *
	 * @param values internal format values
	 */
	public static GenericRow of(Object... values) {
		GenericRow row = new GenericRow(values.length);

		for (int i = 0; i < values.length; ++i) {
			row.setField(i, values[i]);
		}

		return row;
	}
}

