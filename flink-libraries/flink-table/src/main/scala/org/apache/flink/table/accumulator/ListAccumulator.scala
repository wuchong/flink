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

import java.lang.{Iterable => JIterable}


trait ListAccumulator[T] extends Accumulator {

  /**
    * Returns the current value for the state. When the state is not
    * partitioned the returned value is the same for all inputs in a given
    * operator instance. If state partitioning is applied, the value returned
    * depends on the current operator input, as the operator maintains an
    * independent state for each partition.
    *
    *
    * <b>NOTE TO IMPLEMENTERS:</b> if the state is empty, then this method
    * should return `null`.
    * </p>
    *
    * @return The operator state value corresponding to the current input or { @code null}
    *         if the state is empty.
    */
  def get: JIterable[T]

  /**
    * Updates the operator state accessible by [[get()]] by adding the given value
    * to the list of values. The next time [[get()]] is called (for the same state
    * partition) the returned state will represent the updated list.
    *
    * @param value
    * The new value for the state.
    */
  def add(value: T)

}
