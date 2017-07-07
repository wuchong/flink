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

import org.apache.flink.api.common.typeinfo.TypeInfo
import org.apache.flink.table.typeutils.MapAccumulatorTypeInfoFactory

@TypeInfo(classOf[MapAccumulatorTypeInfoFactory[_, _]])
abstract class MapView[K, V] extends Accumulator {

  /**
    * Returns the current value associated with the given key.
    *
    * @param key The key of the mapping
    * @return The value of the mapping with the given key
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def get(key: K): V

  /**
    * Associates a new value with the given key.
    *
    * @param key   The key of the mapping
    * @param value The new value of the mapping
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def put(key: K, value: V): Unit

  /**
    * Copies all of the mappings from the given map into the state.
    *
    * @param map The mappings to be stored in this state
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def putAll(map: util.Map[K, V]): Unit

  /**
    * Deletes the mapping of the given key.
    *
    * @param key The key of the mapping
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def remove(key: K): Unit

  /**
    * Returns whether there exists the given mapping.
    *
    * @param key The key of the mapping
    * @return True if there exists a mapping whose key equals to the given key
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def contains(key: K): Boolean

  /**
    * Returns all the mappings in the state
    *
    * @return An iterable view of all the key-value pairs in the state.
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def entries: JIterable[util.Map.Entry[K, V]]

  /**
    * Returns all the keys in the state
    *
    * @return An iterable view of all the keys in the state.
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def keys: JIterable[K]

  /**
    * Returns all the values in the state.
    *
    * @return An iterable view of all the values in the state.
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def values: JIterable[V]

  /**
    * Iterates over all the mappings in the state.
    *
    * @return An iterator over all the mappings in the state
    * @throws Exception Thrown if the system cannot access the state.
    */
  @throws[Exception]
  def iterator: util.Iterator[util.Map.Entry[K, V]]

  def create(): MapView[K, V] = new HeapMapView[K, V]()
}
