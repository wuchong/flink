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
import org.apache.flink.table.dataformat.BaseArray;
import org.apache.flink.table.dataformat.BinaryArray;
import org.apache.flink.table.dataformat.GenericArray;
import org.apache.flink.table.dataformat.writer.BinaryArrayWriter;
import org.apache.flink.table.dataformat.writer.BinaryWriter;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.utils.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * Serializer for {@link BaseArray}.
 */
@Internal
public class BaseArraySerializer extends TypeSerializer<BaseArray> {
	private static final long serialVersionUID = 1L;

	private final LogicalType eleType;
	private final TypeSerializer<Object> eleSer;

	private transient BinaryArray reuseArray;
	private transient BinaryArrayWriter reuseWriter;

	@SuppressWarnings("unchecked")
	public BaseArraySerializer(LogicalType eleType, ExecutionConfig conf) {
		this.eleType = eleType;
		this.eleSer = (TypeSerializer<Object>) LogicalTypeUtils.internalTypeSerializer(eleType, conf);
	}

	private BaseArraySerializer(LogicalType eleType, TypeSerializer<Object> eleSer) {
		this.eleType = eleType;
		this.eleSer = eleSer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<BaseArray> duplicate() {
		return new BaseArraySerializer(eleType, eleSer.duplicate());
	}

	@Override
	public BaseArray createInstance() {
		return new BinaryArray();
	}

	@Override
	public BaseArray copy(BaseArray from) {
		return from instanceof GenericArray ?
				copyGenericArray((GenericArray) from) :
				((BinaryArray) from).copy();
	}

	@Override
	public BaseArray copy(BaseArray from, BaseArray reuse) {
		return copy(from);
	}

	private GenericArray copyGenericArray(GenericArray array) {
		if (array.isPrimitiveArray()) {
			switch (eleType.getTypeRoot()) {
				case BOOLEAN:
					return new GenericArray(Arrays.copyOf((boolean[]) array.getArray(), array.numElements()));
				case TINYINT:
					return new GenericArray(Arrays.copyOf((byte[]) array.getArray(), array.numElements()));
				case SMALLINT:
					return new GenericArray(Arrays.copyOf((short[]) array.getArray(), array.numElements()));
				case INTEGER:
					return new GenericArray(Arrays.copyOf((int[]) array.getArray(), array.numElements()));
				case BIGINT:
					return new GenericArray(Arrays.copyOf((long[]) array.getArray(), array.numElements()));
				case FLOAT:
					return new GenericArray(Arrays.copyOf((float[]) array.getArray(), array.numElements()));
				case DOUBLE:
					return new GenericArray(Arrays.copyOf((double[]) array.getArray(), array.numElements()));
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
			return new GenericArray(newArray);
		}
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BaseArray record, DataOutputView target) throws IOException {
		BinaryArray binaryArray = toBinaryArray(record);
		target.writeInt(binaryArray.getSizeInBytes());
		SegmentsUtil.copyToView(binaryArray.getSegments(), binaryArray.getOffset(), binaryArray.getSizeInBytes(), target);
	}

	public BinaryArray toBinaryArray(BaseArray from) {
		if (from instanceof BinaryArray) {
			return (BinaryArray) from;
		}

		int numElements = from.numElements();
		if (reuseArray == null) {
			reuseArray = new BinaryArray();
		}
		if (reuseWriter == null || reuseWriter.getNumElements() != numElements) {
			reuseWriter = new BinaryArrayWriter(
					reuseArray, numElements, BinaryArray.calculateFixLengthPartSize(eleType));
		} else {
			reuseWriter.reset();
		}

		for (int i = 0; i < numElements; i++) {
			if (from.isNullAt(i)) {
				reuseWriter.setNullAt(i, eleType);
			} else {
				BinaryWriter.write(reuseWriter, i, BaseArray.get(from, i, eleType), eleType);
			}
		}
		reuseWriter.complete();

		return reuseArray;
	}

	@Override
	public BaseArray deserialize(DataInputView source) throws IOException {
		return deserializeReuse(new BinaryArray(), source);
	}

	@Override
	public BaseArray deserialize(BaseArray reuse, DataInputView source) throws IOException {
		return deserializeReuse(reuse instanceof GenericArray ? new BinaryArray() : (BinaryArray) reuse, source);
	}

	private BinaryArray deserializeReuse(BinaryArray reuse, DataInputView source) throws IOException {
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

		BaseArraySerializer that = (BaseArraySerializer) o;

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
	public TypeSerializerSnapshot<BaseArray> snapshotConfiguration() {
		return new BaseArraySerializerSnapshot(eleType, eleSer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BaseArraySerializer}.
	 */
	public static final class BaseArraySerializerSnapshot implements TypeSerializerSnapshot<BaseArray> {
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
		public TypeSerializer<BaseArray> restoreSerializer() {
			return new BaseArraySerializer(previousType, previousEleSer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<BaseArray> resolveSchemaCompatibility(TypeSerializer<BaseArray> newSerializer) {
			if (!(newSerializer instanceof BaseArraySerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			BaseArraySerializer newBaseArraySerializer = (BaseArraySerializer) newSerializer;
			if (!previousType.equals(newBaseArraySerializer.eleType) ||
				!previousEleSer.equals(newBaseArraySerializer.eleSer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
