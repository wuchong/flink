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
package org.apache.flink.api.table.runtime.aggregate.window.batch

import java.lang.Iterable

import org.apache.flink.api.common.functions.CombineFunction
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.aggregate.Aggregate

import scala.collection.JavaConversions._

/** This groupReduce function only works for time based window on batch tables **/
class WindowAggregateReduceCombineFunction(
    windowKeyPos: Int,
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    aggregates: Array[Aggregate[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowArity: Int,
    finalRowArity: Int)
  extends WindowAggregateReduceGroupFunction(
    windowKeyPos,
    windowSize,
    windowStartPos,
    windowEndPos,
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    intermediateRowArity,
    finalRowArity)
  with CombineFunction[Row, Row] {

  /**
    * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
    *
    * @param records  Sub-grouped intermediate aggregate Rows iterator.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row]): Row = {

    // Initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(aggregateBuffer))

    // Merge intermediate aggregate value to buffer.
    var last: Row = null
    records.foreach((record) => {
      aggregates.foreach(_.merge(record, aggregateBuffer))
      last = record
    })

    // Set group keys to aggregateBuffer.
    for (i <- groupKeysMapping.indices) {
      aggregateBuffer.setField(i, last.productElement(i))
    }

    aggregateBuffer
  }

}
