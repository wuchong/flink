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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.ChangeRow;
import org.apache.flink.table.dataformat.ChangeType;
import org.apache.flink.types.Row;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * .
 */
public class ChangeRowSerializer extends TypeSerializer<ChangeRow> {

	private static final long serialVersionUID = 1L;

	private final RowSerializer rowSerializer;
	private final int arity;

	public ChangeRowSerializer(RowSerializer rowSerializer) {
		this.rowSerializer = checkNotNull(rowSerializer);
		this.arity = rowSerializer.getArity();
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<ChangeRow> duplicate() {
		RowSerializer ser = (RowSerializer) rowSerializer.duplicate();
		return new ChangeRowSerializer(ser);
	}

	@Override
	public ChangeRow createInstance() {
		return new ChangeRow(ChangeType.INSERT, new Row(arity));
	}

	@Override
	public ChangeRow copy(ChangeRow from) {
		return new ChangeRow(from.getChangeType(), rowSerializer.copy(from.getRow()));
	}

	@Override
	public ChangeRow copy(ChangeRow from, ChangeRow reuse) {
		reuse.setChangeType(from.getChangeType());
		reuse.setRow(rowSerializer.copy(from.getRow()));
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ChangeRow record, DataOutputView target) throws IOException {
		int header = record.getChangeType().ordinal();
		target.writeByte(header);
		rowSerializer.serialize(record.getRow(), target);
	}

	@Override
	public ChangeRow deserialize(DataInputView source) throws IOException {
		int header = source.readByte();
		ChangeType changeType = ChangeType.values()[header];
		Row row = rowSerializer.deserialize(source);
		return new ChangeRow(changeType, row);
	}

	@Override
	public ChangeRow deserialize(ChangeRow reuse, DataInputView source) throws IOException {
		int header = source.readByte();
		reuse.setChangeType(ChangeType.values()[header]);
		reuse.setRow(rowSerializer.deserialize(source));
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int header = source.readByte();
		target.writeByte(header);
		rowSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChangeRowSerializer) {
			ChangeRowSerializer other = (ChangeRowSerializer) obj;
			return this.rowSerializer.equals(other.rowSerializer);
		}
		return false;
	}

	@Override
	public int hashCode() {
		return 31 + rowSerializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<ChangeRow> snapshotConfiguration() {
		return new ChangeRowSerializerSnapshot(this);
	}

	// ------------------------------------------------------------------------------------------

	public static final class ChangeRowSerializerSnapshot extends CompositeTypeSerializerSnapshot<ChangeRow, ChangeRowSerializer> {

		private static final int VERSION = 1;

		public ChangeRowSerializerSnapshot() {
			super(ChangeRowSerializer.class);
		}

		public ChangeRowSerializerSnapshot(ChangeRowSerializer serializer) {
			super(serializer);
		}

		@Override
		protected int getCurrentOuterSnapshotVersion() {
			return VERSION;
		}

		@Override
		protected TypeSerializer<?>[] getNestedSerializers(ChangeRowSerializer outerSerializer) {
			return new TypeSerializer[]{outerSerializer.rowSerializer};
		}

		@Override
		protected ChangeRowSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
			return new ChangeRowSerializer((RowSerializer) nestedSerializers[0]);
		}
	}
}
