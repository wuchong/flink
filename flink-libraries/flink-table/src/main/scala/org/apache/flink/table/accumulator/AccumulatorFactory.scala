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
package org.apache.flink.table.accumulator

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, StateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation

abstract class AccumulatorFactory(lazyAccSpecs: Map[String, StateDescriptor[_, _]]) extends Serializable {

  def create(id: String): Accumulator = {
    val spec = lazyAccSpecs.getOrElse(id, throw new RuntimeException(s"Can not find lazy accumulator specific: $id"))
    spec match {
      case map : MapStateDescriptor[_, _] =>
        createMap(map)
      case list : ListStateDescriptor[_] =>
        createList(list)
      case value : ValueStateDescriptor[_] =>
        createValue(value)
      case _ => throw new RuntimeException(s"Unsupported Accumulator type ${spec.getClass.getSimpleName}")
    }
  }

  protected def createValue[T](vsd: ValueStateDescriptor[T]): ValueAccumulator[T]

  protected def createList[T](lsd: ListStateDescriptor[T]): ListAccumulator[T]

  protected def createMap[K, V](msd: MapStateDescriptor[K, V]): MapAccumulator[K, V]

//  protected def createValue[T](id: String, ti: TypeInformation[T]): ValueAccumulator[T]
//
//  protected def createList[T](id: String, ti: TypeInformation[T]): ListAccumulator[T]
//
//  protected def createMap[K, V](id: String, keyType: TypeInformation[K], valueType: TypeInformation[V]): MapAccumulator[K, V]

}
