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

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.aggregate.{Aggregate, TimeWindowPropertyCollector}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, Preconditions}

import scala.collection.JavaConversions._


/** This groupReduce function only works for time based window on batch tables **/
class WindowAggregateReduceGroupFunction(
    rowtimePos: Int,
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    aggregates: Array[Aggregate[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowArity: Int,
    finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  private var collector: TimeWindowPropertyCollector = _
  protected var aggregateBuffer: Row = _
  private var output: Row = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
    collector = new TimeWindowPropertyCollector(windowStartPos, windowEndPos)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // Initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(aggregateBuffer))

    // Merge intermediate aggregate value to buffer.
    var last: Row = null
    records.foreach((record) => {
      aggregates.foreach(_.merge(record, aggregateBuffer))
      last = record
    })

    // Set group keys value to final output.
    groupKeysMapping.foreach {
      case (after, previous) =>
        output.setField(after, last.productElement(previous))
    }

    // Evaluate final aggregate value and set to output.
    aggregateMapping.foreach {
      case (after, previous) =>
        output.setField(after, aggregates(previous).evaluate(aggregateBuffer))
    }

    // get window start timestamp
    val startTs: Long = last.productElement(rowtimePos).asInstanceOf[Long]

    // set collector and window
    collector.wrappedCollector = out
    collector.timeWindow = new TimeWindow(startTs, startTs + windowSize)

    collector.collect(output)
  }

}
