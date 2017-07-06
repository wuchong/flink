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
package org.apache.flink.table.typeutils

import java.lang.reflect.Type
import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{TypeInfoFactory, TypeInformation}
import org.apache.flink.api.common.typeutils.base.{MapSerializer, MapSerializerConfigSnapshot}
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.accumulator.{HeapMapAccumulator, MapAccumulator}

class MapAccumulatorTypeInfo[K, V](val keyType: TypeInformation[K], val valueType: TypeInformation[V])
  extends TypeInformation[MapAccumulator[K, V]] {

  override def isBasicType: Boolean = false

  override def isTupleType: Boolean = false

  override def getArity: Int = 0

  override def getTotalFields: Int = 2

  override def getTypeClass: Class[MapAccumulator[K, V]] = classOf[MapAccumulator[K, V]]

  override def isKeyType: Boolean = false

  override def createSerializer(config: ExecutionConfig): TypeSerializer[MapAccumulator[K, V]] = {
    val keySer = keyType.createSerializer(config)
    val valueSer = valueType.createSerializer(config)
    new MapAccumulatorSerializer[K, V](new MapSerializer[K, V](keySer, valueSer))
  }

  // ----------------------------------------------------------------------------------------------

  override def canEqual(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = 31 * keyType.hashCode + valueType.hashCode

  override def equals(obj: Any): Boolean = canEqual(obj) && {
    obj match {
      case other: MapAccumulatorTypeInfo[_, _] =>
          keyType.equals(other.keyType) &&
          valueType.equals(other.valueType)
      case _ => false
    }
  }

  override def toString: String = s"MapAccumulator<$keyType, $valueType>"
}

class MapAccumulatorSerializer[K, V](mapSerializer: MapSerializer[K, V])
  extends TypeSerializer[MapAccumulator[K, V]] {

  override def isImmutableType: Boolean = mapSerializer.isImmutableType

  override def duplicate(): TypeSerializer[MapAccumulator[K, V]] =
    new MapAccumulatorSerializer[K, V](
      mapSerializer.duplicate().asInstanceOf[MapSerializer[K, V]])

  override def createInstance(): MapAccumulator[K, V] =
    new HeapMapAccumulator[K, V](mapSerializer.createInstance())

  override def copy(from: MapAccumulator[K, V]): MapAccumulator[K, V] = {
    val map = from.asInstanceOf[HeapMapAccumulator[K, V]].map
    new HeapMapAccumulator[K, V](mapSerializer.copy(map))
  }

  override def copy(from: MapAccumulator[K, V], reuse: MapAccumulator[K, V])
  : MapAccumulator[K, V] = copy(from)

  override def getLength: Int = -1  // var length

  override def serialize(record: MapAccumulator[K, V], target: DataOutputView): Unit = {
    val map = record.asInstanceOf[HeapMapAccumulator[K, V]].map
    mapSerializer.serialize(map, target)
  }

  override def deserialize(source: DataInputView): MapAccumulator[K, V] =
    new HeapMapAccumulator[K, V](mapSerializer.deserialize(source))

  override def deserialize(reuse: MapAccumulator[K, V], source: DataInputView)
  : MapAccumulator[K, V] = deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    mapSerializer.copy(source, target)

  override def canEqual(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = mapSerializer.hashCode()

  override def equals(obj: Any): Boolean = canEqual(this) &&
    mapSerializer.equals(obj.asInstanceOf[MapSerializer[_, _]])

  // ----------------------------------------------------------------------------------------

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot =
    mapSerializer.snapshotConfiguration()

  // copy and modified from MapSerializer.ensureCompatibility
  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot)
  : CompatibilityResult[MapAccumulator[K, V]] = {

    configSnapshot match {
      case snapshot: MapSerializerConfigSnapshot[_, _] =>
        val previousKvSerializersAndConfigs = snapshot.getNestedSerializersAndConfigs

        val keyCompatResult = CompatibilityUtil.resolveCompatibilityResult(
          previousKvSerializersAndConfigs.get(0).f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          previousKvSerializersAndConfigs.get(0).f1,
          mapSerializer.getKeySerializer)

        val valueCompatResult = CompatibilityUtil.resolveCompatibilityResult(
          previousKvSerializersAndConfigs.get(1).f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          previousKvSerializersAndConfigs.get(1).f1,
          mapSerializer.getValueSerializer)

        if (!keyCompatResult.isRequiresMigration && !valueCompatResult.isRequiresMigration) {
          CompatibilityResult.compatible[MapAccumulator[K, V]]
        } else if (keyCompatResult.getConvertDeserializer != null
          && valueCompatResult.getConvertDeserializer != null) {
          CompatibilityResult.requiresMigration(
            new MapAccumulatorSerializer[K, V](
              new MapSerializer[K, V](
                new TypeDeserializerAdapter[K](keyCompatResult.getConvertDeserializer),
                new TypeDeserializerAdapter[V](valueCompatResult.getConvertDeserializer))
            )
          )
        } else {
          CompatibilityResult.requiresMigration[MapAccumulator[K, V]]
        }

      case _ => CompatibilityResult.requiresMigration[MapAccumulator[K, V]]
    }
  }
}

class MapAccumulatorTypeInfoFactory[K, V] extends TypeInfoFactory[MapAccumulator[K, V]] {

  override def createTypeInfo(
    t: Type,
    genericParameters: util.Map[String, TypeInformation[_]])
  : TypeInformation[MapAccumulator[K, V]] = {
    var keyType = genericParameters.get("K")
    var valueType = genericParameters.get("V")

    if (keyType == null) {
      keyType = new GenericTypeInfo(classOf[Any])
    }

    if (valueType == null) {
      valueType = new GenericTypeInfo(classOf[Any])
    }

    new MapAccumulatorTypeInfo[K, V](
      keyType.asInstanceOf[TypeInformation[K]],
      valueType.asInstanceOf[TypeInformation[V]])
  }
}
