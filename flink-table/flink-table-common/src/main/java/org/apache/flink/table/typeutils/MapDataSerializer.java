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
import org.apache.flink.table.datastructures.MapData;
import org.apache.flink.table.datastructures.BinaryArrayData;
import org.apache.flink.table.datastructures.BinaryMapData;
import org.apache.flink.table.datastructures.GenericMapData;
import org.apache.flink.table.datastructures.writer.BinaryArrayWriter;
import org.apache.flink.table.datastructures.writer.BinaryWriter;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.table.utils.SegmentsUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializer for {@link MapData}.
 */
@Internal
public class MapDataSerializer extends TypeSerializer<MapData> {
	private static final Logger LOG = LoggerFactory.getLogger(MapDataSerializer.class);
	private static final long serialVersionUID = 1L;

	private final LogicalType keyType;
	private final LogicalType valueType;

	private final TypeSerializer<Object> keySerializer;
	private final TypeSerializer<Object> valueSerializer;

	private transient BinaryArrayData reuseKeyArray;
	private transient BinaryArrayData reuseValueArray;
	private transient BinaryArrayWriter reuseKeyWriter;
	private transient BinaryArrayWriter reuseValueWriter;

	@SuppressWarnings("unchecked")
	public MapDataSerializer(LogicalType keyType, LogicalType valueType, ExecutionConfig conf) {
		this.keyType = keyType;
		this.valueType = valueType;
		this.keySerializer = (TypeSerializer<Object>) LogicalTypeUtils.internalTypeSerializer(keyType, conf);
		this.valueSerializer = (TypeSerializer<Object>) LogicalTypeUtils.internalTypeSerializer(valueType, conf);
	}

	private MapDataSerializer(
			LogicalType keyType,
			LogicalType valueType,
			TypeSerializer<Object> keySerializer,
			TypeSerializer<Object> valueSerializer) {
		this.keyType = keyType;
		this.valueType = valueType;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<MapData> duplicate() {
		return new MapDataSerializer(keyType, valueType, keySerializer.duplicate(), valueSerializer.duplicate());
	}

	@Override
	public MapData createInstance() {
		return new BinaryMapData();
	}

	/**
	 * NOTE: Map should be a HashMap, when we insert the key/value pairs of the TreeMap into
	 * a HashMap, problems maybe occur.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public MapData copy(MapData from) {
		if (from instanceof GenericMapData) {
			Map<Object, Object> fromMap = (Map<Object, Object>) ((GenericMapData) from).getJavaMap();
			if (!(fromMap instanceof HashMap) && LOG.isDebugEnabled()) {
				LOG.debug("It is dangerous to copy a non-HashMap to a HashMap.");
			}
			HashMap<Object, Object> toMap = new HashMap<>();
			for (Map.Entry<Object, Object> entry : fromMap.entrySet()) {
				toMap.put(keySerializer.copy(entry.getKey()), valueSerializer.copy(entry.getValue()));
			}
			return new GenericMapData(toMap);
		} else {
			return ((BinaryMapData) from).copy();
		}
	}

	@Override
	public MapData copy(MapData from, MapData reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(MapData record, DataOutputView target) throws IOException {
		BinaryMapData binaryMap = toBinaryMap(record);
		target.writeInt(binaryMap.getSizeInBytes());
		SegmentsUtil.copyToView(binaryMap.getSegments(), binaryMap.getOffset(), binaryMap.getSizeInBytes(), target);
	}

	@SuppressWarnings("unchecked")
	public BinaryMapData toBinaryMap(MapData from) {
		if (from instanceof BinaryMapData) {
			return (BinaryMapData) from;
		}

		Map<Object, Object> javaMap = (Map<Object, Object>) ((GenericMapData) from).getJavaMap();
		int numElements = javaMap.size();
		if (reuseKeyArray == null) {
			reuseKeyArray = new BinaryArrayData();
		}
		if (reuseValueArray == null) {
			reuseValueArray = new BinaryArrayData();
		}
		if (reuseKeyWriter == null || reuseKeyWriter.getNumElements() != numElements) {
			reuseKeyWriter = new BinaryArrayWriter(
					reuseKeyArray, numElements, BinaryArrayData.calculateFixLengthPartSize(keyType));
		} else {
			reuseKeyWriter.reset();
		}
		if (reuseValueWriter == null || reuseValueWriter.getNumElements() != numElements) {
			reuseValueWriter = new BinaryArrayWriter(
					reuseValueArray, numElements, BinaryArrayData.calculateFixLengthPartSize(valueType));
		} else {
			reuseValueWriter.reset();
		}

		int i = 0;
		for (Map.Entry<Object, Object> entry : javaMap.entrySet()) {
			if (entry.getKey() == null) {
				reuseKeyWriter.setNullAt(i, keyType);
			} else {
				BinaryWriter.write(reuseKeyWriter, i, entry.getKey(), keyType);
			}
			if (entry.getValue() == null) {
				reuseValueWriter.setNullAt(i, valueType);
			} else {
				BinaryWriter.write(reuseValueWriter, i, entry.getValue(), valueType);
			}
			i++;
		}
		reuseKeyWriter.complete();
		reuseValueWriter.complete();

		return BinaryMapData.valueOf(reuseKeyArray, reuseValueArray);
	}

	@Override
	public MapData deserialize(DataInputView source) throws IOException {
		return deserializeReuse(new BinaryMapData(), source);
	}

	@Override
	public MapData deserialize(MapData reuse, DataInputView source) throws IOException {
		return deserializeReuse(reuse instanceof GenericMapData ? new BinaryMapData() : (BinaryMapData) reuse, source);
	}

	private BinaryMapData deserializeReuse(BinaryMapData reuse, DataInputView source) throws IOException {
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

		MapDataSerializer that = (MapDataSerializer) o;

		return keyType.equals(that.keyType) && valueType.equals(that.valueType);
	}

	@Override
	public int hashCode() {
		int result = keyType.hashCode();
		result = 31 * result + valueType.hashCode();
		return result;
	}

	@VisibleForTesting
	public TypeSerializer<Object> getKeySerializer() {
		return keySerializer;
	}

	@VisibleForTesting
	public TypeSerializer<Object> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public TypeSerializerSnapshot<MapData> snapshotConfiguration() {
		return new BaseMapSerializerSnapshot(keyType, valueType, keySerializer, valueSerializer);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link ArrayDataSerializer}.
	 */
	public static final class BaseMapSerializerSnapshot implements TypeSerializerSnapshot<MapData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType previousKeyType;
		private LogicalType previousValueType;

		private TypeSerializer<Object> previousKeySerializer;
		private TypeSerializer<Object> previousValueSerializer;

		@SuppressWarnings("unused")
		public BaseMapSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		BaseMapSerializerSnapshot(
				LogicalType keyT,
				LogicalType valueT,
				TypeSerializer<Object> keySer,
				TypeSerializer<Object> valueSer) {
			this.previousKeyType = keyT;
			this.previousValueType = valueT;

			this.previousKeySerializer = keySer;
			this.previousValueSerializer = valueSer;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			DataOutputViewStream outStream = new DataOutputViewStream(out);
			InstantiationUtil.serializeObject(outStream, previousKeyType);
			InstantiationUtil.serializeObject(outStream, previousValueType);
			InstantiationUtil.serializeObject(outStream, previousKeySerializer);
			InstantiationUtil.serializeObject(outStream, previousValueSerializer);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			try {
				DataInputViewStream inStream = new DataInputViewStream(in);
				this.previousKeyType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousValueType = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousKeySerializer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
				this.previousValueSerializer = InstantiationUtil.deserializeObject(inStream, userCodeClassLoader);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}

		@Override
		public TypeSerializer<MapData> restoreSerializer() {
			return new MapDataSerializer(
				previousKeyType, previousValueType, previousKeySerializer, previousValueSerializer);
		}

		@Override
		public TypeSerializerSchemaCompatibility<MapData> resolveSchemaCompatibility(TypeSerializer<MapData> newSerializer) {
			if (!(newSerializer instanceof MapDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			MapDataSerializer newMapDataSerializer = (MapDataSerializer) newSerializer;
			if (!previousKeyType.equals(newMapDataSerializer.keyType) ||
				!previousValueType.equals(newMapDataSerializer.valueType) ||
				!previousKeySerializer.equals(newMapDataSerializer.keySerializer) ||
				!previousValueSerializer.equals(newMapDataSerializer.valueSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
