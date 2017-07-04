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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.accumulator.MapAccumulator
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment}
import org.apache.flink.table.api.scala.stream.utils.StreamITCase.RetractingSink
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests of groupby (without window) aggregations
  */
class GroupAggregationsITCase extends StreamingWithStateTestBase {
  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))

  @Test
  def testNonKeyedGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
            .select('a.sum, 'b.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("231,91")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1", "2,5", "3,15", "4,34", "5,65", "6,111")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDoubleGroupAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.count as 'cnt, 'b)
      .groupBy('cnt)
      .select('cnt, 'b.count as 'freq)

    val results = t.toRetractStream[Row](queryConfig)

    results.addSink(new RetractingSink)
    env.execute()
    val expected = List("1,1", "2,1", "3,1", "4,1", "5,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new RetractingSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1", "7,1,4,2", "2,1,3,2",
      "3,2,3,3", "1,2,3,3", "14,2,5,1",
      "12,3,5,1", "5,3,4,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregate2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "A"))
    data.+=((2, 2L, "B"))
    data.+=((3, 2L, "B"))
    data.+=((4, 3L, "C"))
    data.+=((5, 3L, "C"))
    data.+=((6, 3L, "C"))
    data.+=((7, 4L, "B"))
    data.+=((8, 4L, "A"))
    data.+=((9, 4L, "D"))
    data.+=((10, 4L, "E"))
    data.+=((11, 5L, "A"))
    data.+=((12, 5L, "B"))

    val distinct = new DistinctCount

    val t = env.fromCollection(data).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, distinct('c))

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1", "2,1", "3,1", "4,4", "5,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

}

class MyACC {

  var map: MapAccumulator[String, java.lang.Integer] = _

  var count: Int = 0

}

class DistinctCount extends AggregateFunction[Long, MyACC] {

  override def createAccumulator(): MyACC = new MyACC

  def accumulate(accumulator: MyACC, id: String): Unit = {
    if (!accumulator.map.contains(id)) {
      accumulator.map.put(id, 1)
      accumulator.count += 1
    }
  }

  override def getValue(accumulator: MyACC): Long = accumulator.count
}
