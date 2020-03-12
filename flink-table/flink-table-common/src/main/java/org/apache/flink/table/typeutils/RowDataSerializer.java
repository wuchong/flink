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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformats.RowData;
import org.apache.flink.table.dataformats.BinaryRowData;
import org.apache.flink.table.dataformats.GenericRowData;
import org.apache.flink.table.dataformats.writer.BinaryRowWriter;
import org.apache.flink.table.dataformats.writer.BinaryWriter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Arrays;

/**
 * Serializer for BaseRow.
 */
@Internal
public class RowDataSerializer extends TypeSerializer<RowData> {
	private static final long serialVersionUID = 1L;

	private BinaryRowDataSerializer binarySerializer;
	private final LogicalType[] types;
	private final TypeSerializer[] fieldSerializers;

	private transient BinaryRowData reuseRow;
	private transient BinaryRowWriter reuseWriter;

	public RowDataSerializer(ExecutionConfig config, RowType rowType) {
		this(rowType.getChildren().toArray(new LogicalType[0]),
			rowType.getChildren().stream()
				.map((LogicalType type) -> LogicalTypeUtils.internalTypeSerializer(type, config))
				.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(ExecutionConfig config, LogicalType... types) {
		this(types, Arrays.stream(types)
			.map((LogicalType type) -> LogicalTypeUtils.internalTypeSerializer(type, config))
			.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
		this.types = types;
		this.fieldSerializers = fieldSerializers;
		this.binarySerializer = new BinaryRowDataSerializer(types.length);
	}

	@Override
	public TypeSerializer<RowData> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowDataSerializer(types, duplicateFieldSerializers);
	}

	@Override
	public RowData createInstance() {
		// default use binary row to deserializer
		return new BinaryRowData(types.length);
	}

	@Override
	public void serialize(RowData row, DataOutputView target) throws IOException {
		binarySerializer.serialize(toBinaryRow(row), target);
	}

	@Override
	public RowData deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryRowData) {
			return binarySerializer.deserialize((BinaryRowData) reuse, source);
		} else {
			return binarySerializer.deserialize(source);
		}
	}

	@Override
	public RowData copy(RowData from) {
		if (from.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
					", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return ((BinaryRowData) from).copy();
		} else {
			return copyBaseRow(from, new GenericRowData(from.getArity()));
		}
	}

	@Override
	public RowData copy(RowData from, RowData reuse) {
		if (from.getArity() != types.length || reuse.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
					", Ruese Row arity: " + reuse.getArity() +
					", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return reuse instanceof BinaryRowData
					? ((BinaryRowData) from).copy((BinaryRowData) reuse)
					: ((BinaryRowData) from).copy();
		} else {
			return copyBaseRow(from, reuse);
		}
	}

	private RowData copyBaseRow(RowData from, RowData reuse) {
		GenericRowData ret;
		if (reuse instanceof GenericRowData) {
			ret = (GenericRowData) reuse;
		} else {
			ret = new GenericRowData(from.getArity());
		}
		ret.setChangelogKind(from.getChangelogKind());
		for (int i = 0; i < from.getArity(); i++) {
			if (!from.isNullAt(i)) {
				ret.setField(
						i,
						fieldSerializers[i].copy((RowData.get(from, i, types[i])))
				);
			} else {
				ret.setField(i, null);
			}
		}
		return ret;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		binarySerializer.copy(source, target);
	}

	public int getArity() {
		return types.length;
	}

	/**
	 * Convert base row to binary row.
	 * TODO modify it to code gen.
	 */
	public BinaryRowData toBinaryRow(RowData row) {
		if (row instanceof BinaryRowData) {
			return (BinaryRowData) row;
		}
		if (reuseRow == null) {
			reuseRow = new BinaryRowData(types.length);
			reuseWriter = new BinaryRowWriter(reuseRow);
		}
		reuseWriter.reset();
		reuseWriter.writeChangelogKind(row.getChangelogKind());
		for (int i = 0; i < types.length; i++) {
			if (row.isNullAt(i)) {
				reuseWriter.setNullAt(i);
			} else {
				BinaryWriter.write(reuseWriter, i, RowData.get(row, i, types[i]), types[i]);
			}
		}
		reuseWriter.complete();
		return reuseRow;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RowDataSerializer) {
			RowDataSerializer other = (RowDataSerializer) obj;
			return Arrays.equals(types, other.types);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
		return new BaseRowSerializerSnapshot(types, fieldSerializers);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}.
	 */
	public static final class BaseRowSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType[] previousTypes;
		private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

		@SuppressWarnings("unused")
		public BaseRowSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseRowSerializerSnapshot(LogicalType[] types, TypeSerializer<Object>[] serializers) {
			this.previousTypes = types;
			this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(
					serializers);
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousTypes.length);
			DataOutputViewStream stream = new DataOutputViewStream(out);
			for (LogicalType previousType : previousTypes) {
				InstantiationUtil.serializeObject(stream, previousType);
			}
			nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
				throws IOException {
			int length = in.readInt();
			DataInputViewStream stream = new DataInputViewStream(in);
			previousTypes = new LogicalType[length];
			for (int i = 0; i < length; i++) {
				try {
					previousTypes[i] = InstantiationUtil.deserializeObject(
							stream,
							userCodeClassLoader
					);
				}
				catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			}
			this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
					in,
					userCodeClassLoader
			);
		}

		@Override
		public RowDataSerializer restoreSerializer() {
			return new RowDataSerializer(
					previousTypes,
					nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		@Override
		public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(TypeSerializer<RowData> newSerializer) {
			if (!(newSerializer instanceof RowDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
			if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> intermediateResult =
					CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
							newRowSerializer.fieldSerializers,
							nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots()
					);

			if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
				RowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
						reconfiguredCompositeSerializer);
			}

			return intermediateResult.getFinalResult();
		}
	}
}
