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

import java.util
import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state._

private[flink] class StateAccumulatorFactory(ctx: RuntimeContext, lazyAccSpecs: Map[String, StateDescriptor[_, _]])
  extends AccumulatorFactory(lazyAccSpecs) {

  override protected def createValue[T](vsd: ValueStateDescriptor[T]): ValueAccumulator[T] = {
    val state = ctx.getState(vsd)
    new StateValueAccumulator(state)
  }

  override protected def createList[T](lsd: ListStateDescriptor[T]): ListAccumulator[T] = {
    val state = ctx.getListState(lsd)
    new StateListAccumulator(state)
  }

  override protected def createMap[K, V](msd: MapStateDescriptor[K, V]): MapAccumulator[K, V] = {
    val state = ctx.getMapState(msd)
    new StateMapAccumulator(state)
  }
}

private[flink] class StateMapAccumulator[K, V](state: MapState[K, V]) extends MapAccumulator[K, V] {

  override def get(key: K): V = state.get(key)

  override def put(key: K, value: V): Unit = state.put(key, value)

  override def putAll(map: util.Map[K, V]): Unit = state.putAll(map)

  override def remove(key: K): Unit = state.remove(key)

  override def contains(key: K): Boolean = state.contains(key)

  override def entries: JIterable[util.Map.Entry[K, V]] = state.entries()

  override def keys: JIterable[K] = state.keys()

  override def values: JIterable[V] = state.values()

  override def iterator: util.Iterator[util.Map.Entry[K, V]] = state.iterator()

  override def clear(): Unit = state.clear()
}


private[flink] class StateListAccumulator[T](state: ListState[T]) extends ListAccumulator[T] {

  override def get: JIterable[T] = state.get()

  override def add(value: T): Unit = state.add(value)

  override def clear(): Unit = state.clear()
}


private[flink] class StateValueAccumulator[T](state: ValueState[T]) extends ValueAccumulator[T] {

  override def get: T = state.value()

  override def set(value: T): Unit = state.update(value)

  override def clear(): Unit = state.clear()
}
