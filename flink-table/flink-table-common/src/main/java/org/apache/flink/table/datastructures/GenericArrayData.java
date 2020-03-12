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

package org.apache.flink.table.datastructures;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Objects;

/**
 * A GenericArray is an array where all the elements have the same type.
 * It can be considered as a wrapper class of the normal java array.
 */
@PublicEvolving
public final class GenericArrayData implements ArrayData {

	private static final long serialVersionUID = 1L;

	private final Object arr;
	private final int numElements;
	private final boolean isPrimitiveArray;

	public GenericArrayData(Object[] array) {
		this(array, array.length, false);
	}

	public GenericArrayData(int[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArrayData(long[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArrayData(float[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true); }

	public GenericArrayData(double[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArrayData(short[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArrayData(byte[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArrayData(boolean[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	private GenericArrayData(Object arr, int numElements, boolean isPrimitiveArray) {
		this.arr = arr;
		this.numElements = numElements;
		this.isPrimitiveArray = isPrimitiveArray;
	}

	public boolean isPrimitiveArray() {
		return isPrimitiveArray;
	}

	public Object getArray() {
		return arr;
	}

	@Override
	public int numElements() {
		return numElements;
	}

	@Override
	public boolean isNullAt(int pos) {
		return !isPrimitiveArray && ((Object[]) arr)[pos] == null;
	}

	@Override
	public boolean[] toBooleanArray() {
		return (boolean[]) arr;
	}

	@Override
	public byte[] toByteArray() {
		return (byte[]) arr;
	}

	@Override
	public short[] toShortArray() {
		return (short[]) arr;
	}

	@Override
	public int[] toIntArray() {
		return (int[]) arr;
	}

	@Override
	public long[] toLongArray() {
		return (long[]) arr;
	}

	@Override
	public float[] toFloatArray() {
		return (float[]) arr;
	}

	@Override
	public double[] toDoubleArray() {
		return (double[]) arr;
	}

	@Override
	public boolean getBoolean(int pos) {
		return isPrimitiveArray ? ((boolean[]) arr)[pos] : (boolean) getObject(pos);
	}

	@Override
	public byte getByte(int pos) {
		return isPrimitiveArray ? ((byte[]) arr)[pos] : (byte) getObject(pos);
	}

	@Override
	public short getShort(int pos) {
		return isPrimitiveArray ? ((short[]) arr)[pos] : (short) getObject(pos);
	}

	@Override
	public int getInt(int pos) {
		return isPrimitiveArray ? ((int[]) arr)[pos] : (int) getObject(pos);
	}

	@Override
	public long getLong(int pos) {
		return isPrimitiveArray ? ((long[]) arr)[pos] : (long) getObject(pos);
	}

	@Override
	public float getFloat(int pos) {
		return isPrimitiveArray ? ((float[]) arr)[pos] : (float) getObject(pos);
	}

	@Override
	public double getDouble(int pos) {
		return isPrimitiveArray ? ((double[]) arr)[pos] : (double) getObject(pos);
	}

	@Override
	public byte[] getBinary(int pos) {
		return (byte[]) getObject(pos);
	}

	@Override
	public StringData getString(int pos) {
		return (StringData) getObject(pos);
	}

	@Override
	public DecimalData getDecimal(int pos, int precision, int scale) {
		return (DecimalData) getObject(pos);
	}

	@Override
	public TimestampData getTimestamp(int pos, int precision) {
		return (TimestampData) getObject(pos);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> RawValueData<T> getGeneric(int pos) {
		return (RawValueData<T>) getObject(pos);
	}

	@Override
	public RowData getRow(int pos, int numFields) {
		return (RowData) getObject(pos);
	}

	@Override
	public ArrayData getArray(int pos) {
		return (ArrayData) getObject(pos);
	}

	@Override
	public MapData getMap(int pos) {
		return (MapData) getObject(pos);
	}

	private Object getObject(int pos) {
		return ((Object[]) arr)[pos];
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GenericArrayData that = (GenericArrayData) o;
		return numElements == that.numElements &&
			isPrimitiveArray == that.isPrimitiveArray &&
			Objects.equals(arr, that.arr);
	}

	@Override
	public int hashCode() {
		return Objects.hash(arr, numElements, isPrimitiveArray);
	}
}
