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

import java.util.Objects;

/**
 * A GenericArray is an array where all the elements have the same type.
 * It can be considered as a wrapper class of the normal java array.
 */
@PublicEvolving
public class GenericArray implements BaseArray {

	private static final long serialVersionUID = 1L;

	private final Object arr;
	private final int numElements;
	private final boolean isPrimitiveArray;

	public GenericArray(Object[] array) {
		this(array, array.length, false);
	}

	public GenericArray(int[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArray(long[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArray(float[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true); }

	public GenericArray(double[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArray(short[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArray(byte[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	public GenericArray(boolean[] primitiveArray) {
		this(primitiveArray, primitiveArray.length, true);
	}

	private GenericArray(Object arr, int numElements, boolean isPrimitiveArray) {
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
	public BinaryString getString(int pos) {
		return (BinaryString) getObject(pos);
	}

	@Override
	public Decimal getDecimal(int pos, int precision, int scale) {
		return (Decimal) getObject(pos);
	}

	@Override
	public SqlTimestamp getTimestamp(int pos, int precision) {
		return (SqlTimestamp) getObject(pos);
	}

	@Override
	public <T> BinaryGeneric<T> getGeneric(int pos) {
		return (BinaryGeneric) getObject(pos);
	}

	@Override
	public BaseRow getRow(int pos, int numFields) {
		return (BaseRow) getObject(pos);
	}

	@Override
	public BaseArray getArray(int pos) {
		return (BaseArray) getObject(pos);
	}

	@Override
	public BaseMap getMap(int pos) {
		return (BaseMap) getObject(pos);
	}

	private Object getObject(int pos) {
		return ((Object[]) arr)[pos];
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GenericArray that = (GenericArray) o;
		return numElements == that.numElements &&
			isPrimitiveArray == that.isPrimitiveArray &&
			Objects.equals(arr, that.arr);
	}

	@Override
	public int hashCode() {
		return Objects.hash(arr, numElements, isPrimitiveArray);
	}
}
