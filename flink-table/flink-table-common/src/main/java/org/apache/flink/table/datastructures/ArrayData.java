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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

/**
 * {@link ArrayData} is an internal data structure represents data of {@link ArrayType}
 * in Flink Table/SQL, which only contains elements of the internal data structures.
 *
 * <p>There are different implementations depending on the scenario:
 * After serialization, it becomes the {@link BinaryArrayData} format.
 * Convenient updates use the {@link GenericArrayData} format.
 */
@PublicEvolving
public interface ArrayData extends TypedGetters, Serializable {

	int numElements();

	boolean[] toBooleanArray();

	byte[] toByteArray();

	short[] toShortArray();

	int[] toIntArray();

	long[] toLongArray();

	float[] toFloatArray();

	double[] toDoubleArray();

	// ------------------------------------------------------------------------------------------

	static Object get(ArrayData array, int ordinal, LogicalType type) {
		return TypedGetters.get(array, ordinal, type);
	}
}
