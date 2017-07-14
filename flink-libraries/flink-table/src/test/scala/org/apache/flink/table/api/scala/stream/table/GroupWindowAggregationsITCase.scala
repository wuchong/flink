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

import java.lang.{Integer => JInt, Boolean => JBoolean, Double => JDouble}
import java.util.Comparator
import java.util.{ArrayList => JArrayList, List => JList}

import com.google.common.collect.MinMaxPriorityQueue
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{ListTypeInfo, ObjectArrayTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment, Types}
import org.apache.flink.table.api.java.utils.UserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.api.java.utils.UserDefinedOperators.{EvenFilterOperator, JoinOperator, TopN}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.table.GroupWindowAggregationsITCase.TimestampAndWatermarkWithOffset
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.functions.{AggregateFunction, OperatorFunction}
import org.apache.flink.table.functions.aggfunctions.CountAggFunction
import org.apache.flink.table.typeutils.MultisetTypeInfo
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.collection.JavaConversions._

/**
  * We only test some aggregations until better testing of constructed DataStream
  * programs is possible.
  */
class GroupWindowAggregationsITCase extends StreamingMultipleProgramsTestBase {
  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))


  @Test
  def testEventTimeTumblingWindow2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = List(
      (1L, 1, "GO"),
      (2L, 2, "JAVA"),
      (2L, 1, "JAVA"),
      (3L, 5, "GO"),
      (3L, 5, "C++"),
      (3L, 5, "C++"),
      (3L, 2, "GO"),
      (4L, 2, ".NET"),
      (8L, 3, ".NET"),
      (16L, 3, ".NET"))

    val stream = env
                 .fromCollection(data)
                 .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val topn = new TopN(3)

    val windowedTable = table
                        .window(Tumble over 5.milli on 'rowtime as 'w)
                        .groupBy('w)
                        .select('w.start as 'wstart, topn('string, 'int) as 'agg)
//                        .select('wstart, 'agg.get("f0"))

    windowedTable.printSchema()

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1970-01-01 00:00:00.0,[(C++,10), (GO,8), (JAVA,3)]",
      "1970-01-01 00:00:00.005,[(.NET,3)]",
      "1970-01-01 00:00:00.015,[(.NET,3)]")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test
  def testJoinOperator(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data: List[(JInt, String, JInt, JDouble, JBoolean)] = List(
      (1, "apple", 0, 0.0, true),
      (0, null, 1, 12.1, false),
      (2, "orange", 0, 0.0, true),
      (4, "pencil", 0, 0.0, true),
      (0, null, 3, 45.1, false),
      (0, null, 4, 11.1, false))

    val stream = env
      .fromCollection(data)
    val table = stream.toTable(tEnv, 'id1, 'itemName, 'id2, 'itemPrice, 'leftOrRight)

    val join = new JoinOperator

    val res = table
      .join(join('id1, 'itemName, 'id2, 'itemPrice, 'leftOrRight) as ('id, 'name, 'price))
      .select('id, 'name, 'price)

    val results = res.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "1,apple,12.1", "4,pencil,11.1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEvenFilter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val data = List(
      (1, "apple"),
      (2, "orange"),
      (3, "pencil"),
      (4, "iphone"),
      (5, "mac"))

    val stream = env
      .fromCollection(data)
    val table = stream.toTable(tEnv, 'id, 'name)

    val even = new EvenFilterOperator

    val res = table
      .join(even('id, 'name) as ('newId, 'newName))
      .select('newId, 'newName)

    val results = res.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "2,orange", "4,iphone")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('int), 'int.avg,
              weightAvgFun('long, 'int), weightAvgFun('int, 'int))

    val results = windowedTable.toAppendStream[Row](queryConfig)
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("Hello world,1,3,8,3", "Hello world,2,3,12,3", "Hello,1,2,2,2",
                       "Hello,2,2,3,2", "Hi,1,1,1,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    //To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestdata = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvgWithMerge

    val stream = env
      .fromCollection(sessionWindowTestdata)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Session withGap 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('int), 'int.avg,
              weightAvgFun('long, 'int), weightAvgFun('int, 'int))

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("Hello World,1,9,9,9", "Hello,1,16,16,16", "Hello,4,3,5,5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'proctime.proctime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w)
      .select(countFun('string), 'int.avg,
              weightAvgFun('long, 'int), weightAvgFun('int, 'int))

    val results = windowedTable.toAppendStream[Row](queryConfig)
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("2,1,1,1", "2,2,6,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testEventTimeTumblingWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env
      .fromCollection(data)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset(0L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)
    val countFun = new CountAggFunction
    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Tumble over 5.milli on 'rowtime as 'w)
      .groupBy('w, 'string)
      .select('string, countFun('string), 'int.avg, weightAvgFun('long, 'int),
              weightAvgFun('int, 'int), 'int.min, 'int.max, 'int.sum, 'w.start, 'w.end)

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq(
      "Hello world,1,3,8,3,3,3,3,1970-01-01 00:00:00.005,1970-01-01 00:00:00.01",
      "Hello world,1,3,16,3,3,3,3,1970-01-01 00:00:00.015,1970-01-01 00:00:00.02",
      "Hello,2,2,3,2,2,2,4,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005",
      "Hi,1,1,1,1,1,1,1,1970-01-01 00:00:00.0,1970-01-01 00:00:00.005")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val weightAvgFun = new WeightedAvg

    val windowedTable = table
      .window(Slide over 2.rows every 1.rows on 'proctime as 'w)
      .groupBy('w, 'int2, 'int3, 'string)
      .select(weightAvgFun('long, 'int))

    val results = windowedTable.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = Seq("12", "8", "2", "3", "1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}

object GroupWindowAggregationsITCase {
  class TimestampAndWatermarkWithOffset(
    offset: Long) extends AssignerWithPunctuatedWatermarks[(Long, Int, String)] {

    override def checkAndGetNextWatermark(
        lastElement: (Long, Int, String),
        extractedTimestamp: Long)
      : Watermark = {
      new Watermark(extractedTimestamp - offset)
    }

    override def extractTimestamp(
        element: (Long, Int, String),
        previousElementTimestamp: Long): Long = {
      element._1
    }
  }
}
