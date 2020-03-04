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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentWritable;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.typeutils.BinaryRowSerializer;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Paged Serializer for {@link BinaryRow}.
 */
public final class BinaryRowPagedSerializer extends BinaryRowSerializer {

	private static final long serialVersionUID = 1L;

	public BinaryRowPagedSerializer(int numFields) {
		super(numFields);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BinaryRow> duplicate() {
		return new BinaryRowPagedSerializer(numFields);
	}

	// ============================ Page related operations ===================================

	public int serializeToPages(
			BinaryRow record,
			AbstractPagedOutputView headerLessView) throws IOException {
		checkArgument(headerLessView.getHeaderLength() == 0);
		int skip = checkSkipWriteForFixLengthPart(headerLessView);
		headerLessView.writeInt(record.getSizeInBytes());
		serializeWithoutLength(record, headerLessView);
		return skip;
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

	public BinaryRow deserializeFromPages(
			AbstractPagedInputView headerLessView) throws IOException {
		return deserializeFromPages(createInstance(), headerLessView);
	}

	public BinaryRow deserializeFromPages(
			BinaryRow reuse,
			AbstractPagedInputView headerLessView) throws IOException {
		checkArgument(headerLessView.getHeaderLength() == 0);
		checkSkipReadForFixLengthPart(headerLessView);
		return deserialize(reuse, headerLessView);
	}

	public BinaryRow mapFromPages(AbstractPagedInputView headerLessView) throws IOException {
		return mapFromPages(createInstance(), headerLessView);
	}

	public BinaryRow mapFromPages(
			BinaryRow reuse,
			AbstractPagedInputView headerLessView) throws IOException {
		checkArgument(headerLessView.getHeaderLength() == 0);
		checkSkipReadForFixLengthPart(headerLessView);
		pointTo(headerLessView.readInt(), reuse, headerLessView);
		return reuse;
	}

	/**
	 * Copy a binaryRow which stored in paged input view to output view.
	 *
	 * @param source source paged input view where the binary row stored
	 * @param target the target output view.
	 */
	public void copyFromPagesToView(
			AbstractPagedInputView source, DataOutputView target) throws IOException {
		checkSkipReadForFixLengthPart(source);
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	/**
	 * Point row to memory segments with offset(in the AbstractPagedInputView) and length.
	 *
	 * @param length         row length.
	 * @param reuse          reuse BinaryRow object.
	 * @param headerLessView source memory segments container.
	 */
	public void pointTo(int length, BinaryRow reuse, AbstractPagedInputView headerLessView)
			throws IOException {
		checkArgument(headerLessView.getHeaderLength() == 0);
		if (length < 0) {
			throw new IOException(String.format(
					"Read unexpected bytes in source of positionInSegment[%d] and limitInSegment[%d]",
					headerLessView.getCurrentPositionInSegment(),
					headerLessView.getCurrentSegmentLimit()
			));
		}

		int remainInSegment = headerLessView.getCurrentSegmentLimit()
				- headerLessView.getCurrentPositionInSegment();
		MemorySegment currSeg = headerLessView.getCurrentSegment();
		int currPosInSeg = headerLessView.getCurrentPositionInSegment();
		if (remainInSegment >= length) {
			// all in one segment, that's good.
			reuse.pointTo(currSeg, currPosInSeg, length);
			headerLessView.skipBytesToRead(length);
		} else {
			pointToMultiSegments(
					reuse,
					headerLessView,
					length,
					length - remainInSegment,
					currSeg,
					currPosInSeg
			);
		}
	}

	private void pointToMultiSegments(
			BinaryRow reuse, AbstractPagedInputView source, int sizeInBytes,
			int remainLength, MemorySegment currSeg, int currPosInSeg) throws IOException {

		int segmentSize = currSeg.size();
		int div = remainLength / segmentSize;
		int remainder = remainLength - segmentSize * div; // equal to p % q
		int varSegSize = remainder == 0 ? div : div + 1;

		MemorySegment[] segments = new MemorySegment[varSegSize + 1];
		segments[0] = currSeg;
		for (int i = 1; i <= varSegSize; i++) {
			source.advance();
			segments[i] = source.getCurrentSegment();
		}

		// The remaining is 0. There is no next Segment at this time. The current Segment is
		// all the data of this row, so we need to skip segmentSize bytes to read. We can't
		// jump directly to the next Segment. Because maybe there are no segment in later.
		int remainLenInLastSeg = remainder == 0 ? segmentSize : remainder;
		source.skipBytesToRead(remainLenInLastSeg);
		reuse.pointTo(segments, currPosInSeg, sizeInBytes);
	}

	/**
	 * We need skip bytes to write when the remain bytes of current segment is not
	 * enough to write binary row fixed part.
	 * See {@link BinaryRow}.
	 */
	private int checkSkipWriteForFixLengthPart(AbstractPagedOutputView out) throws IOException {
		// skip if there is no enough size.
		int available = out.getSegmentSize() - out.getCurrentPositionInSegment();
		if (available < getSerializedRowFixedPartLength()) {
			out.advance();
			return available;
		}
		return 0;
	}

	/**
	 * We need skip bytes to read when the remain bytes of current segment is not
	 * enough to write binary row fixed part. See {@link BinaryRow}.
	 */
	public void checkSkipReadForFixLengthPart(AbstractPagedInputView source) throws IOException {
		// skip if there is no enough size.
		// Note: Use currentSegmentLimit instead of segmentSize.
		int available = source.getCurrentSegmentLimit() - source.getCurrentPositionInSegment();
		if (available < getSerializedRowFixedPartLength()) {
			source.advance();
		}
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
	public boolean equals(Object obj) {
		return obj instanceof BinaryRowPagedSerializer
				&& numFields == ((BinaryRowPagedSerializer) obj).numFields;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(numFields);
	}

	@Override
	public TypeSerializerSnapshot<BinaryRow> snapshotConfiguration() {
		return new BinaryRowPagedSerializerSnapshot(numFields);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowPagedSerializer}.
	 */
	public static final class BinaryRowPagedSerializerSnapshot
			implements TypeSerializerSnapshot<BinaryRow> {
		private static final int CURRENT_VERSION = 3;

		private int previousNumFields;

		@SuppressWarnings("unused")
		public BinaryRowPagedSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BinaryRowPagedSerializerSnapshot(int numFields) {
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
			return new BinaryRowPagedSerializer(previousNumFields);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BinaryRow> resolveSchemaCompatibility(
				TypeSerializer<BinaryRow> newSerializer) {
			if (!(newSerializer instanceof BinaryRowPagedSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BinaryRowPagedSerializer newBinaryRowSerializer = (BinaryRowPagedSerializer) newSerializer;
			if (previousNumFields != newBinaryRowSerializer.numFields) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}

}
