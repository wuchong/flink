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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.datastructures.ArrayData;
import org.apache.flink.table.datastructures.BinaryArrayData;
import org.apache.flink.table.datastructures.GenericArrayData;
import org.apache.flink.table.datastructures.writer.BinaryArrayWriter;
import org.apache.flink.table.datastructures.writer.BinaryWriter;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.utils.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Serializer for {@link ArrayData}.
 */
@Internal
public class ArrayDataSerializer extends TypeSerializer<ArrayData> {
	private static final long serialVersionUID = 1L;

	private final LogicalType eleType;
	private final TypeSerializer<Object> eleSer;

	private transient BinaryArrayData reuseArray;
	private transient BinaryArrayWriter reuseWriter;

	@SuppressWarnings("unchecked")
	public ArrayDataSerializer(LogicalType eleType, ExecutionConfig conf) {
		this.eleType = eleType;
		this.eleSer = (TypeSerializer<Object>) LogicalTypeUtils.internalTypeSerializer(eleType, conf);
	}

	private ArrayDataSerializer(LogicalType eleType, TypeSerializer<Object> eleSer) {
		this.eleType = eleType;
		this.eleSer = eleSer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<ArrayData> duplicate() {
		return new ArrayDataSerializer(eleType, eleSer.duplicate());
	}

	@Override
	public ArrayData createInstance() {
		return new BinaryArrayData();
	}

	@Override
	public ArrayData copy(ArrayData from) {
		return from instanceof GenericArrayData ?
				copyGenericArray((GenericArrayData) from) :
				((BinaryArrayData) from).copy();
	}

	@Override
	public ArrayData copy(ArrayData from, ArrayData reuse) {
		return copy(from);
	}

	private GenericArrayData copyGenericArray(GenericArrayData array) {
		if (array.isPrimitiveArray()) {
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return new GenericArrayData(Arrays.copyOf((boolean[]) array.getArray(), array.numElements()));
				case TINYINT:
					return new GenericArrayData(Arrays.copyOf((byte[]) array.getArray(), array.numElements()));
				case SMALLINT:
					return new GenericArrayData(Arrays.copyOf((short[]) array.getArray(), array.numElements()));
				case INTEGER:
					return new GenericArrayData(Arrays.copyOf((int[]) array.getArray(), array.numElements()));
				case BIGINT:
					return new GenericArrayData(Arrays.copyOf((long[]) array.getArray(), array.numElements()));
				case FLOAT:
					return new GenericArrayData(Arrays.copyOf((float[]) array.getArray(), array.numElements()));
				case DOUBLE:
					return new GenericArrayData(Arrays.copyOf((double[]) array.getArray(), array.numElements()));
				default:
					throw new RuntimeException("Unknown type: " + eleType);
			}
		} else {
			Object[] objectArray = (Object[]) array.getArray();
			Object[] newArray = (Object[]) Array.newInstance(
					LogicalTypeUtils.internalConversionClass(eleType),
					array.numElements());
			for (int i = 0; i < array.numElements(); i++) {
				newArray[i] = eleSer.copy(objectArray[i]);
			}
			return new GenericArrayData(newArray);
		}
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(ArrayData record, DataOutputView target) throws IOException {
		BinaryArrayData binaryArray = toBinaryArray(record);
		target.writeInt(binaryArray.getSizeInBytes());
		SegmentsUtil.copyToView(binaryArray.getSegments(), binaryArray.getOffset(), binaryArray.getSizeInBytes(), target);
	}

	public BinaryArrayData toBinaryArray(ArrayData from) {
		if (from instanceof BinaryArrayData) {
			return (BinaryArrayData) from;
		}

		int numElements = from.numElements();
		if (reuseArray == null) {
			reuseArray = new BinaryArrayData();
		}
		if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
			reuseWriter = new BinaryArrayWriter(
					reuseArray, numElements, BinaryArrayData.calculateFixLengthPartSize(eleType));
		} else {
			reuseWriter.reset();
		}

		for (int i = 0; i < numElements; i++) {
			if (from.isNullAt(i)) {
				reuseWriter.setNullAt(i, eleType);
			} else {
				BinaryWriter.write(reuseWriter, i, ArrayData.get(from, i, eleType), eleType);
			}
		}
		reuseWriter.complete();

		return reuseArray;
	}

	@Override
	public ArrayData deserialize(DataInputView source) throws IOException {
		return deserializeReuse(new BinaryArrayData(), source);
	}

	@Override
	public ArrayData deserialize(ArrayData reuse, DataInputView source) throws IOException {
		return deserializeReuse(reuse instanceof GenericArrayData ? new BinaryArrayData() : (BinaryArrayData) reuse, source);
	}

	private BinaryArrayData deserializeReuse(BinaryArrayData reuse, DataInputView source) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		reuse.pointTo(MemorySegmentFactory.wrap(bytes), 0, bytes.length);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		target.writeInt(length);
		target.write(source, length);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		ArrayDataSerializer that = (ArrayDataSerializer) o;

		return eleType.equals(that.eleType);
	}

	@Override
	public int hashCode() {
		return eleType.hashCode();
	}

	@VisibleForTesting
	public TypeSerializer getEleSer() {
		return eleSer;
	}

	@Override
	public TypeSerializerSnapshot<ArrayData> snapshotConfiguration() {
		return new BaseArraySerializerSnapshot(eleType, eleSer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}.
	 */
	public static final class BaseArraySerializerSnapshot implements TypeSerializerSnapshot<ArrayData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType previousType;
		private TypeSerializer previousEleSer;

		@SuppressWarnings("unused")
		public BaseArraySerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseArraySerializerSnapshot(LogicalType eleType, TypeSerializer eleSer) {
			this.previousType = eleType;
			this.previousEleSer = eleSer;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			DataOutputViewStream outStream = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(outStream, previousType);
			InstantiationUtil.serializeObject(outStream, previousEleSer);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			try {
				DataInputViewStream inStream = new DataInputViewStream(in);
				this.previousType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousEleSer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		public TypeSerializer<ArrayData> restoreSerializer() {
			return new ArrayDataSerializer(previousType, previousEleSer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<ArrayData> resolveSchemaCompatibility(TypeSerializer<ArrayData> newSerializer) {
			if (!(newSerializer instanceof ArrayDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			ArrayDataSerializer newArrayDataSerializer = (ArrayDataSerializer) newSerializer;
			if (!previousType.equals(newArrayDataSerializer.eleType) ||
				!previousEleSer.equals(newArrayDataSerializer.eleSer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
