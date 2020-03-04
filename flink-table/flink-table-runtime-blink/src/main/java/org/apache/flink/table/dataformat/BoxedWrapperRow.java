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

package org.apache.flink.table.dataformat;

import org.apache.flink.types.BooleanValue;
import org.apache.flink.types.ByteValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BaseRow} that wrap primitive type to boxed object to reuse.
 */
public final class BoxedWrapperRow implements BaseRow, TypedSetters {
	private static final long serialVersionUID = 1L;

	/** The array to store the actual internal format values. */
	private final Object[] fields;
	/** The changelog kind of this row. */
	private ChangelogKind kind;

	public BoxedWrapperRow(int arity) {
		this.fields = new Object[arity];
		this.kind = ChangelogKind.INSERT; // INSERT as default
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
	public void setNullAt(int ordinal) {
		this.fields[ordinal] = null;
	}

	@Override
	public boolean getBoolean(int i) {
		return ((BooleanValue) fields[i]).getValue();
	}

	@Override
	public byte getByte(int i) {
		return ((ByteValue) fields[i]).getValue();
	}

	@Override
	public short getShort(int i) {
		return ((ShortValue) fields[i]).getValue();
	}

	@Override
	public int getInt(int i) {
		return ((IntValue) fields[i]).getValue();
	}

	@Override
	public long getLong(int i) {
		return ((LongValue) fields[i]).getValue();
	}

	@Override
	public float getFloat(int i) {
		return ((FloatValue) fields[i]).getValue();
	}

	@Override
	public double getDouble(int i) {
		return ((DoubleValue) fields[i]).getValue();
	}

	@Override
	public BinaryString getString(int ordinal) {
		return (BinaryString) this.fields[ordinal];
	}

	@Override
	public byte[] getBinary(int ordinal) {
		return (byte[]) this.fields[ordinal];
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
	public Decimal getDecimal(int ordinal, int precision, int scale) {
		return (Decimal) this.fields[ordinal];
	}

	@Override
	public SqlTimestamp getTimestamp(int ordinal, int precision) {
		return (SqlTimestamp) this.fields[ordinal];
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> BinaryGeneric<T> getGeneric(int ordinal) {
		return (BinaryGeneric<T>) this.fields[ordinal];
	}

	@Override
	public BaseRow getRow(int ordinal, int numFields) {
		return (BaseRow) this.fields[ordinal];
	}

	@Override
	public void setBoolean(int i, boolean value) {
		BooleanValue wrap;
		if ((wrap = (BooleanValue) fields[i]) == null) {
			wrap = new BooleanValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setByte(int i, byte value) {
		ByteValue wrap;
		if ((wrap = (ByteValue) fields[i]) == null) {
			wrap = new ByteValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setShort(int i, short value) {
		ShortValue wrap;
		if ((wrap = (ShortValue) fields[i]) == null) {
			wrap = new ShortValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setInt(int i, int value) {
		IntValue wrap;
		if ((wrap = (IntValue) fields[i]) == null) {
			wrap = new IntValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setLong(int i, long value) {
		LongValue wrap;
		if ((wrap = (LongValue) fields[i]) == null) {
			wrap = new LongValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setFloat(int i, float value) {
		FloatValue wrap;
		if ((wrap = (FloatValue) fields[i]) == null) {
			wrap = new FloatValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setDouble(int i, double value) {
		DoubleValue wrap;
		if ((wrap = (DoubleValue) fields[i]) == null) {
			wrap = new DoubleValue();
			fields[i] = wrap;
		}
		wrap.setValue(value);
	}

	@Override
	public void setDecimal(int i, Decimal value, int precision) {
		this.fields[i] = value;
	}

	@Override
	public void setTimestamp(int ordinal, SqlTimestamp value, int precision) {
		this.fields[ordinal] = value;
	}

	public void setNonPrimitiveValue(int i, Object value) {
		fields[i] = value;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		sb.append(String.format("%1$-13s", kind.toString()));
		sb.append("|");
		for (int i = 0; i < fields.length; i++) {
			if (i != 0) {
				sb.append(",");
			}
			sb.append(StringUtils.arrayAwareToString(fields[i]));
		}
		sb.append(")");
		return sb.toString();
	}

	@Override
	public int hashCode() {
		return 31 * getChangelogKind().hashCode() + Arrays.hashCode(fields);
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof BoxedWrapperRow) {
			BoxedWrapperRow other = (BoxedWrapperRow) o;
			return kind == other.kind && Arrays.equals(fields, other.fields);
		} else {
			return false;
		}
	}
}
