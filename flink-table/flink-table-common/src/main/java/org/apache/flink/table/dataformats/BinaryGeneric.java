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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.utils.SegmentsUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Internal format to generic.
 */
public final class BinaryGeneric<T> extends LazyBinaryFormat<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	public BinaryGeneric(T javaObject) {
		super(javaObject);
	}

	public BinaryGeneric(MemorySegment[] segments, int offset, int sizeInBytes) {
		super(segments, offset, sizeInBytes);
	}

	public BinaryGeneric(MemorySegment[] segments, int offset, int sizeInBytes, T javaObject) {
		super(segments, offset, sizeInBytes, javaObject);
	}

	@Override
	protected BinarySection materialize(TypeSerializer<T> serializer) {
		try {
			byte[] bytes = InstantiationUtil.serializeToByteArray(serializer, javaObject);
			return new BinarySection(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, 0, bytes.length);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	static <T> BinaryGeneric<T> readBinaryGenericFieldFromSegments(
			MemorySegment[] segments, int baseOffset, long offsetAndSize) {
		final int size = ((int) offsetAndSize);
		int offset = (int) (offsetAndSize >> 32);
		return new BinaryGeneric<>(segments, offset + baseOffset, size, null);
	}

	public static <T> T getJavaObjectFromBinaryGeneric(BinaryGeneric<T> value, TypeSerializer<T> ser) {
		if (value.getJavaObject() == null) {
			try {
				value.setJavaObject(InstantiationUtil.deserializeFromByteArray(ser,
						SegmentsUtil.copyToBytes(value.getSegments(), value.getOffset(), value.getSizeInBytes())));
			} catch (IOException e) {
				throw new FlinkRuntimeException(e);
			}
		}
		return value.getJavaObject();
	}

	@Override
	public boolean equals(Object o) {
		throw new UnsupportedOperationException("BinaryGeneric cannot be compared");
	}

	@Override
	public int hashCode() {
		throw new UnsupportedOperationException("BinaryGeneric does not have a hashCode");
	}

	@Override
	public String toString() {
		return "BinaryGeneric{" +
			"javaObject=" + javaObject +
			'}';
	}
}
