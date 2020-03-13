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

package org.apache.flink.table.datastructures;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.utils.SegmentsUtil;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link DecimalData} is an internal data structure represents data of {@link DecimalType}
 * in Flink Table/SQL.
 *
 * <p>It is an immutable implementation which can hold a long if values are small enough.
 *
 * <p>The semantics of the fields are as follows:
 * - precision and scale represent the SQL precision and scale we are looking for
 * - If decimalVal is set, it represents the whole decimal value
 * - Otherwise, the decimal value is longVal / (10 ** scale).
 */
@PublicEvolving
public final class DecimalData implements Comparable<DecimalData>, Serializable {

	private static final long serialVersionUID = 1L;

	// member fields and static fields are package-visible,
	// in order to be accessible for DecimalDataUtils

	static final int MAX_COMPACT_PRECISION = 18;

	/**
	 * Maximum number of decimal digits an Int can represent. (1e9 < Int.MaxValue < 1e10)
	 */
	static final int MAX_INT_DIGITS = 9;

	/**
	 * Maximum number of decimal digits a Long can represent. (1e18 < Long.MaxValue < 1e19)
	 */
	static final int MAX_LONG_DIGITS = 18;

	static final long[] POW10 = new long[MAX_COMPACT_PRECISION + 1];

	static {
		POW10[0] = 1;
		for (int i = 1; i < POW10.length; i++) {
			POW10[i] = 10 * POW10[i - 1];
		}
	}

	// for now, we follow closely to what Spark does.
	// see if we can improve upon it later.

	// (precision, scale) is always correct.
	// if precision > MAX_COMPACT_PRECISION,
	//   `decimalVal` represents the value. `longVal` is undefined
	// otherwise, (longVal, scale) represents the value
	//   `decimalVal` may be set and cached

	final int precision;
	final int scale;

	final long longVal;
	BigDecimal decimalVal;

	// this constructor does not perform any sanity check.
	DecimalData(int precision, int scale, long longVal, BigDecimal decimalVal) {
		this.precision = precision;
		this.scale = scale;
		this.longVal = longVal;
		this.decimalVal = decimalVal;
	}

	// ------------------------------------------------------------------------------------------
	// Public Interfaces
	// ------------------------------------------------------------------------------------------

	public int getPrecision() {
		return precision;
	}

	public int getScale() {
		return scale;
	}

	public BigDecimal toBigDecimal() {
		BigDecimal bd = decimalVal;
		if (bd == null) {
			decimalVal = bd = BigDecimal.valueOf(longVal, scale);
		}
		return bd;
	}

	/**
	 * Returns a long whose value is the <i>unscaled value</i> of this {@code DecimalData}.
	 *
	 * @return the unscaled value of this {@code DecimalData}.
	 * @throws ArithmeticException if the value of {@code this} will not exactly fit in a {@code long}.
	 */
	public long toUnscaledLong() {
		if (isCompact()) {
			return longVal;
		} else {
			return toBigDecimal().unscaledValue().longValueExact();
		}
	}

	/**
	 * Returns a byte array whose value is the <i>unscaled value</i> of this {@code DecimalData}.
	 *
	 * @return the unscaled value of this {@code DecimalData}.
	 */
	public byte[] toUnscaledBytes() {
		if (!isCompact()) {
			return toBigDecimal().unscaledValue().toByteArray();
		}

		// big endian; consistent with BigInteger.toByteArray()
		byte[] bytes = new byte[8];
		long l = longVal;
		for (int i = 0; i < 8; i++) {
			bytes[7 - i] = (byte) l;
			l >>>= 8;
		}
		return bytes;
	}

	/**
	 * Returns whether the decimal data is small enough to be stored in a long.
	 */
	public boolean isCompact() {
		return isCompact(this.precision);
	}

	public DecimalData copy() {
		return new DecimalData(precision, scale, longVal, decimalVal);
	}

	@Override
	public int hashCode() {
		return toBigDecimal().hashCode();
	}

	@Override
	public int compareTo(@Nonnull DecimalData that) {
		if (isCompact(this.precision) && isCompact(that.precision) && this.scale == that.scale) {
			return Long.compare(this.longVal, that.longVal);
		}
		return this.toBigDecimal().compareTo(that.toBigDecimal());
	}

	@Override
	public boolean equals(final Object o) {
		if (!(o instanceof DecimalData)) {
			return false;
		}
		DecimalData that = (DecimalData) o;
		return this.compareTo(that) == 0;
	}

	@Override
	public String toString() {
		return toBigDecimal().toPlainString();
	}

	// ------------------------------------------------------------------------------------------
	// Constructor helper
	// ------------------------------------------------------------------------------------------

	// convert external BigDecimal to internal representation.
	// first, value may be rounded to have the desired `scale`
	// then `precision` is checked. if precision overflow, it will return `null`
	public static DecimalData fromBigDecimal(BigDecimal bd, int precision, int scale) {
		bd = bd.setScale(scale, RoundingMode.HALF_UP);
		if (bd.precision() > precision) {
			return null;
		}

		long longVal = -1;
		if (precision <= MAX_COMPACT_PRECISION) {
			longVal = bd.movePointRight(scale).longValueExact();
		}
		return new DecimalData(precision, scale, longVal, bd);
	}

	public static DecimalData fromUnscaledLong(int precision, int scale, long longVal) {
		checkArgument(precision > 0 && precision <= MAX_LONG_DIGITS);
		checkArgument((longVal >= 0 ? longVal : -longVal) < POW10[precision]);
		return new DecimalData(precision, scale, longVal, null);
	}

	// we assume the bytes were generated by us from toUnscaledBytes()
	public static DecimalData fromUnscaledBytes(int precision, int scale, byte[] bytes) {
		if (precision > MAX_COMPACT_PRECISION) {
			BigDecimal bd = new BigDecimal(new BigInteger(bytes), scale);
			return new DecimalData(precision, scale, -1, bd);
		}
		assert bytes.length == 8;
		long l = 0;
		for (int i = 0; i < 8; i++) {
			l <<= 8;
			l |= (bytes[i] & (0xff));
		}
		return new DecimalData(precision, scale, l, null);
	}

	public static DecimalData zero(int precision, int scale) {
		if (precision <= MAX_COMPACT_PRECISION) {
			return new DecimalData(precision, scale, 0, null);
		} else {
			return fromBigDecimal(BigDecimal.ZERO, precision, scale);
		}
	}

	// ------------------------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------------------------

	public static boolean isCompact(int precision) {
		return precision <= MAX_COMPACT_PRECISION;
	}

	// ------------------------------------------------------------------------------------------
	// Package Visible Utilities
	// ------------------------------------------------------------------------------------------

	static DecimalData readDecimalFieldFromSegments(
		MemorySegment[] segments,
		int baseOffset,
		long offsetAndSize,
		int precision,
		int scale) {
		final int size = ((int) offsetAndSize);
		int subOffset = (int) (offsetAndSize >> 32);
		byte[] bytes = new byte[size];
		SegmentsUtil.copyToBytes(segments, baseOffset + subOffset, bytes, 0, size);
		return fromUnscaledBytes(precision, scale, bytes);
	}
}
