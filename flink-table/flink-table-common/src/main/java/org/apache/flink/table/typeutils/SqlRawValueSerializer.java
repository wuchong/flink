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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.dataformats.LazyBinarySqlRawValue;
import org.apache.flink.table.dataformats.SqlRawValue;
import org.apache.flink.table.utils.SegmentsUtil;

import java.io.IOException;

/**
 * Serializer for {@link SqlRawValue}.
 */
@Internal
public final class SqlRawValueSerializer<T> extends TypeSerializer<SqlRawValue<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> serializer;

	public SqlRawValueSerializer(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public SqlRawValue<T> createInstance() {
		return new LazyBinarySqlRawValue<>(serializer.createInstance());
	}

	@Override
	public SqlRawValue<T> copy(SqlRawValue<T> from) {
		LazyBinarySqlRawValue<T> rawValue = (LazyBinarySqlRawValue<T>) from;
		rawValue.ensureMaterialized(serializer);
		byte[] bytes = SegmentsUtil.copyToBytes(rawValue.getSegments(), rawValue.getOffset(), rawValue.getSizeInBytes());
		T newJavaObject = rawValue.getJavaObject() == null ? null : serializer.copy(rawValue.getJavaObject());
		return new LazyBinarySqlRawValue<>(
			new MemorySegment[]{MemorySegmentFactory.wrap(bytes)},
			0,
			bytes.length,
			newJavaObject);
	}

	@Override
	public SqlRawValue<T> copy(SqlRawValue<T> from, SqlRawValue<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(SqlRawValue<T> record, DataOutputView target) throws IOException {
		LazyBinarySqlRawValue<T> rawValue = (LazyBinarySqlRawValue<T>) record;
		rawValue.ensureMaterialized(serializer);
		target.writeInt(rawValue.getSizeInBytes());
		SegmentsUtil.copyToView(rawValue.getSegments(), rawValue.getOffset(), rawValue.getSizeInBytes(), target);
	}

	@Override
	public SqlRawValue<T> deserialize(DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		return new LazyBinarySqlRawValue<>(
				new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
				0,
				bytes.length);
	}

	@Override
	public SqlRawValue<T> deserialize(SqlRawValue<T> record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public SqlRawValueSerializer<T> duplicate() {
		return new SqlRawValueSerializer<>(serializer.duplicate());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SqlRawValueSerializer that = (SqlRawValueSerializer) o;

		return serializer.equals(that.serializer);
	}

	@Override
	public int hashCode() {
		return serializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<SqlRawValue<T>> snapshotConfiguration() {
		return new SqlRawValueSerializerSnapshot<>(this);
	}

	public TypeSerializer<T> getInnerSerializer() {
		return serializer;
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link SqlRawValueSerializer}.
	 */
	public static final class SqlRawValueSerializerSnapshot<T> extends CompositeTypeSerializerSnapshot<SqlRawValue<T>, SqlRawValueSerializer<T>> {

		@SuppressWarnings("unused")
		public SqlRawValueSerializerSnapshot() {
			super(SqlRawValueSerializer.class);
		}

		public SqlRawValueSerializerSnapshot(SqlRawValueSerializer<T> serializerInstance) {
			super(serializerInstance);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return 0;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(SqlRawValueSerializer<T> outerSerializer) {
			return new TypeSerializer[]{outerSerializer.serializer};
		}

		@Override
		@SuppressWarnings("unchecked")
		protected SqlRawValueSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new SqlRawValueSerializer<>((TypeSerializer<T>) nestedSerializers[0]);
		}
	}
}
