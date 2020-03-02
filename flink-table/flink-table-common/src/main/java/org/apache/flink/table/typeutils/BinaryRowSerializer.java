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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentWritable;
import org.apache.flink.table.dataformats.BinaryRow;
import org.apache.flink.table.utils.SegmentsUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Serializer for {@link BinaryRow}.
 */
@Internal
public class BinaryRowSerializer extends TypeSerializer<BinaryRow> {

	private static final long serialVersionUID = 1L;
	public static final int LENGTH_SIZE_IN_BYTES = 4;

	private final int numFields;
	private final int fixedLengthPartSize;

	public BinaryRowSerializer(int numFields) {
		this.numFields = numFields;
		this.fixedLengthPartSize = BinaryRow.calculateFixPartSizeInBytes(numFields);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BinaryRow> duplicate() {
		return new BinaryRowSerializer(numFields);
	}

	@Override
	public BinaryRow createInstance() {
		return new BinaryRow(numFields);
	}

	@Override
	public BinaryRow copy(BinaryRow from) {
		return copy(from, new BinaryRow(numFields));
	}

	@Override
	public BinaryRow copy(BinaryRow from, BinaryRow reuse) {
		return from.copy(reuse);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryRow record, DataOutputView target) throws IOException {
		target.writeInt(record.getSizeInBytes());
		if (target instanceof MemorySegmentWritable) {
			serializeWithoutLength(record, (MemorySegmentWritable) target);
		} else {
			SegmentsUtil.copyToView(
					record.getSegments(), record.getOffset(),
					record.getSizeInBytes(), target);
		}
	}

	@Override
	public BinaryRow deserialize(DataInputView source) throws IOException {
		BinaryRow row = new BinaryRow(numFields);
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		row.pointTo(MemorySegmentFactory.wrap(bytes), 0, length);
		return row;
	}

	@Override
	public BinaryRow deserialize(BinaryRow reuse, DataInputView source) throws IOException {
		MemorySegment[] segments = reuse.getSegments();
		checkArgument(segments == null || (segments.length == 1 && reuse.getOffset() == 0),
				"Reuse BinaryRow should have no segments or only one segment and offset start at 0.");

		int length = source.readInt();
		if (segments == null || segments[0].size() < length) {
			segments = new MemorySegment[]{MemorySegmentFactory.wrap(new byte[length])};
		}
		source.readFully(segments[0].getArray(), 0, length);
		reuse.pointTo(segments, 0, length);
		return reuse;
	}

	public int getArity() {
		return numFields;
	}

	private static void serializeWithoutLength(
			BinaryRow record, MemorySegmentWritable writable) throws IOException {
		if (record.getSegments().length == 1) {
			writable.write(record.getSegments()[0], record.getOffset(), record.getSizeInBytes());
		} else {
			serializeWithoutLengthSlow(record, writable);
		}
	}

	public static void serializeWithoutLengthSlow(
			BinaryRow record, MemorySegmentWritable out) throws IOException {
		int remainSize = record.getSizeInBytes();
		int posInSegOfRecord = record.getOffset();
		int segmentSize = record.getSegments()[0].size();
		for (MemorySegment segOfRecord : record.getSegments()) {
			int nWrite = Math.min(segmentSize - posInSegOfRecord, remainSize);
			assert nWrite > 0;
			out.write(segOfRecord, posInSegOfRecord, nWrite);

			// next new segment.
			posInSegOfRecord = 0;
			remainSize -= nWrite;
			if (remainSize == 0) {
				break;
			}
		}
		checkArgument(remainSize == 0);
	}

	/**
	 * Return fixed part length to serialize one row.
	 */
	public int getSerializedRowFixedPartLength() {
		return getFixedLengthPartSize() + LENGTH_SIZE_IN_BYTES;
	}

	public int getFixedLengthPartSize() {
		return fixedLengthPartSize;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof BinaryRowSerializer
				&& numFields == ((BinaryRowSerializer) obj).numFields;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(numFields);
	}

	@Override
	public TypeSerializerSnapshot<BinaryRow> snapshotConfiguration() {
		return new BinaryRowSerializerSnapshot(numFields);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowSerializer}.
	 */
	public static final class BinaryRowSerializerSnapshot
			implements TypeSerializerSnapshot<BinaryRow> {
		private static final int CURRENT_VERSION = 3;

		private int previousNumFields;

		@SuppressWarnings("unused")
		public BinaryRowSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BinaryRowSerializerSnapshot(int numFields) {
			this.previousNumFields = numFields;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousNumFields);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
				throws IOException {
			this.previousNumFields = in.readInt();
		}

		@Override
		public TypeSerializer<BinaryRow> restoreSerializer() {
			return new BinaryRowSerializer(previousNumFields);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BinaryRow> resolveSchemaCompatibility(
				TypeSerializer<BinaryRow> newSerializer) {
			if (!(newSerializer instanceof BinaryRowSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BinaryRowSerializer newBinaryRowSerializer = (BinaryRowSerializer) newSerializer;
			if (previousNumFields != newBinaryRowSerializer.numFields) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}

}
