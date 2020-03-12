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
import org.apache.flink.table.dataformats.SqlRow;
import org.apache.flink.table.dataformats.BinaryRow;
import org.apache.flink.table.dataformats.GenericRow;
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
public class BaseRowSerializer extends TypeSerializer<SqlRow> {
	private static final long serialVersionUID = 1L;

	private BinaryRowSerializer binarySerializer;
	private final LogicalType[] types;
	private final TypeSerializer[] fieldSerializers;

	private transient BinaryRow reuseRow;
	private transient BinaryRowWriter reuseWriter;

	public BaseRowSerializer(ExecutionConfig config, RowType rowType) {
		this(rowType.getChildren().toArray(new LogicalType[0]),
			rowType.getChildren().stream()
				.map((LogicalType type) -> LogicalTypeUtils.internalTypeSerializer(type, config))
				.toArray(TypeSerializer[]::new));
	}

	public BaseRowSerializer(ExecutionConfig config, LogicalType... types) {
		this(types, Arrays.stream(types)
			.map((LogicalType type) -> LogicalTypeUtils.internalTypeSerializer(type, config))
			.toArray(TypeSerializer[]::new));
	}

	public BaseRowSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
		this.types = types;
		this.fieldSerializers = fieldSerializers;
		this.binarySerializer = new BinaryRowSerializer(types.length);
	}

	@Override
	public TypeSerializer<SqlRow> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new BaseRowSerializer(types, duplicateFieldSerializers);
	}

	@Override
	public SqlRow createInstance() {
		// default use binary row to deserializer
		return new BinaryRow(types.length);
	}

	@Override
	public void serialize(SqlRow row, DataOutputView target) throws IOException {
		binarySerializer.serialize(toBinaryRow(row), target);
	}

	@Override
	public SqlRow deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public SqlRow deserialize(SqlRow reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryRow) {
			return binarySerializer.deserialize((BinaryRow) reuse, source);
		} else {
			return binarySerializer.deserialize(source);
		}
	}

	@Override
	public SqlRow copy(SqlRow from) {
		if (from.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
					", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRow) {
			return ((BinaryRow) from).copy();
		} else {
			return copyBaseRow(from, new GenericRow(from.getArity()));
		}
	}

	@Override
	public SqlRow copy(SqlRow from, SqlRow reuse) {
		if (from.getArity() != types.length || reuse.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
					", Ruese Row arity: " + reuse.getArity() +
					", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRow) {
			return reuse instanceof BinaryRow
					? ((BinaryRow) from).copy((BinaryRow) reuse)
					: ((BinaryRow) from).copy();
		} else {
			return copyBaseRow(from, reuse);
		}
	}

	private SqlRow copyBaseRow(SqlRow from, SqlRow reuse) {
		GenericRow ret;
		if (reuse instanceof GenericRow) {
			ret = (GenericRow) reuse;
		} else {
			ret = new GenericRow(from.getArity());
		}
		ret.setChangelogKind(from.getChangelogKind());
		for (int i = 0; i < from.getArity(); i++) {
			if (!from.isNullAt(i)) {
				ret.setField(
						i,
						fieldSerializers[i].copy((SqlRow.get(from, i, types[i])))
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
	public BinaryRow toBinaryRow(SqlRow row) {
		if (row instanceof BinaryRow) {
			return (BinaryRow) row;
		}
		if (reuseRow == null) {
			reuseRow = new BinaryRow(types.length);
			reuseWriter = new BinaryRowWriter(reuseRow);
		}
		reuseWriter.reset();
		reuseWriter.writeChangelogKind(row.getChangelogKind());
		for (int i = 0; i < types.length; i++) {
			if (row.isNullAt(i)) {
				reuseWriter.setNullAt(i);
			} else {
				BinaryWriter.write(reuseWriter, i, SqlRow.get(row, i, types[i]), types[i]);
			}
		}
		reuseWriter.complete();
		return reuseRow;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BaseRowSerializer) {
			BaseRowSerializer other = (BaseRowSerializer) obj;
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
	public TypeSerializerSnapshot<SqlRow> snapshotConfiguration() {
		return new BaseRowSerializerSnapshot(types, fieldSerializers);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowSerializer}.
	 */
	public static final class BaseRowSerializerSnapshot implements TypeSerializerSnapshot<SqlRow> {
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
		public BaseRowSerializer restoreSerializer() {
			return new BaseRowSerializer(
					previousTypes,
					nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		@Override
		public TypeSerializerSchemaCompatibility<SqlRow> resolveSchemaCompatibility(TypeSerializer<SqlRow> newSerializer) {
			if (!(newSerializer instanceof BaseRowSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BaseRowSerializer newRowSerializer = (BaseRowSerializer) newSerializer;
			if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<SqlRow> intermediateResult =
					CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
							newRowSerializer.fieldSerializers,
							nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots()
					);

			if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
				BaseRowSerializer reconfiguredCompositeSerializer = restoreSerializer();
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
						reconfiguredCompositeSerializer);
			}

			return intermediateResult.getFinalResult();
		}
	}
}
