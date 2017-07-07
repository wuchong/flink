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

import java.lang.reflect.Field

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, StateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation

private[flink] trait AccumulatorSpec[ACC <: Accumulator] {
  def id: String
  def field: Field
  def toStateDescriptor: StateDescriptor[_, _]
}

private[flink] case class ValueAccumulatorSpec[T](
    id: String,
    ti: TypeInformation[T],
    field: Field)
  extends AccumulatorSpec[ValueAccumulator[T]] {
  override def toStateDescriptor: StateDescriptor[_, _] = new ValueStateDescriptor[T](id, ti)
}

private[flink] case class ListAccumulatorSpec[T](
    id: String,
    ti: TypeInformation[T],
    field: Field)
  extends AccumulatorSpec[ListAccumulator[T]] {
  override def toStateDescriptor: StateDescriptor[_, _] = new ListStateDescriptor[T](id, ti)
}

private[flink] case class MapAccumulatorSpec[K, V](
    id: String,
    key: TypeInformation[K],
    value: TypeInformation[V],
    field: Field)
  extends AccumulatorSpec[MapView[K, V]] {
  override def toStateDescriptor: StateDescriptor[_, _] = new MapStateDescriptor[K, V](id, key, value)
}
