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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.utils.SegmentsUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Internal format for raw values.
 * @param <T> the java type of the raw value.
 */
@Internal
public final class BinaryRawValueData<T> extends LazyBinaryFormat<T>
		implements RawValueData<T>, Serializable {

	private static final long serialVersionUID = 1L;

	public BinaryRawValueData(T javaObject) {
		super(javaObject);
	}

	public BinaryRawValueData(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryRawValueData(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	// ------------------------------------------------------------------------------------------
	// Public Interfaces
	// ------------------------------------------------------------------------------------------

	@Override
	public T getJavaObject(TypeSerializer<T> serializer) {
		if (javaObject == null) {
			try {
				javaObject = InstantiationUtil.deserializeFromByteArray(
					serializer,
					getBytes(serializer));
			} catch (IOException e) {
				throw new FlinkRuntimeException(e);
			}
		}
		return javaObject;
	}

	@Override
	public byte[] getBytes(TypeSerializer<T> serializer) {
		ensureMaterialized(serializer);
		return SegmentsUtil.copyToBytes(getSegments(), getOffset(), getSizeInBytes());
	}

	@Override
	public boolean equals(Object o) {
		throw new UnsupportedOperationException("LazyBinarySqlRawValue cannot be compared");
	}

	@Override
	public int hashCode() {
		throw new UnsupportedOperationException("LazyBinarySqlRawValue does not have a hashCode");
	}

	@Override
	public String toString() {
		return String.format("SqlRawValue{%s}", javaObject == null ? "?" : javaObject);
	}
	
	// ------------------------------------------------------------------------------------
	// Internal methods
	// ------------------------------------------------------------------------------------

	@Override
	protected BinarySection materialize(TypeSerializer<T> serializer) {
		try {
			byte[] bytes = InstantiationUtil.serializeToByteArray(serializer, javaObject);
			return new BinarySection(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static <T> BinaryRawValueData<T> readRawValueFieldFromSegments(
		MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		return new BinaryRawValueData<>(segments, offset + baseOffset, size, null);
	}
}
