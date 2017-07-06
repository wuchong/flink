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

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, StateDescriptor, ValueStateDescriptor}


private[flink] class HeapAccumulatorFactory(lazyAccSpecs: Map[String, StateDescriptor[_, _]])
  extends AccumulatorFactory(lazyAccSpecs) {

  override protected def createValue[T](vsd: ValueStateDescriptor[T]): ValueAccumulator[T] =
    new HeapValueAccumulator[T]

  override protected def createList[T](lsd: ListStateDescriptor[T]): ListAccumulator[T] =
    new HeapListAccumulator[T]

  override protected def createMap[K, V](msd: MapStateDescriptor[K, V]): MapAccumulator[K, V] =
    new HeapMapAccumulator[K, V]
}

private[flink] class HeapMapAccumulator[K, V](initialMap: util.Map[K, V]) extends MapAccumulator[K, V] {

  def this() = this(new util.HashMap[K, V]())

  val map: util.Map[K, V] = initialMap

  override def get(key: K): V = map.get(key)

  override def put(key: K, value: V): Unit = map.put(key, value)

  override def putAll(map: util.Map[K, V]): Unit = this.map.putAll(map)

  override def remove(key: K): Unit = map.remove(key)

  override def contains(key: K): Boolean = map.containsKey(key)

  override def entries: JIterable[util.Map.Entry[K, V]] = map.entrySet()

  override def keys: JIterable[K] = map.keySet()

  override def values: JIterable[V] = map.values()

  override def iterator: util.Iterator[util.Map.Entry[K, V]] = map.entrySet().iterator()

  override def clear(): Unit = map.clear()
}


private[flink] class HeapListAccumulator[T](initialList: util.List[T]) extends ListAccumulator[T] {

  def this() = this(new util.ArrayList[T]())

  private val list: util.List[T] = initialList

  override def get: JIterable[T] = list

  override def add(value: T): Unit = list.add(value)

  override def clear(): Unit = list.clear()
}


private[flink] class HeapValueAccumulator[T](initialValue: Option[T]) extends ValueAccumulator[T] {

  def this() = this(None)

  private var value: Option[T] = initialValue

  override def get: T = {
    if (value.isDefined) {
      value.get
    } else {
      null.asInstanceOf[T]
    }
  }

  override def set(value: T): Unit = this.value = Some(value)

  override def clear(): Unit = value = None

}
