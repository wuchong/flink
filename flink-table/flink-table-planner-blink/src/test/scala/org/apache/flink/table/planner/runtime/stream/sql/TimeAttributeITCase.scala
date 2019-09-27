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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import java.math.BigDecimal
import java.sql.Timestamp
import java.util.TimeZone

import scala.collection.JavaConverters._

class TimeAttributeITCase extends StreamingTestBase {

  val data = List(
    row(utcTimestamp(1L), 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    row(utcTimestamp(2L), 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    row(utcTimestamp(3L), 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    row(utcTimestamp(4L), 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    row(utcTimestamp(7L), 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    row(utcTimestamp(8L), 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    row(utcTimestamp(16L), 4, 4d, 4f, new BigDecimal("4"), "Hello world"))

  def utcTimestamp(ts: Long): Timestamp = {
    new Timestamp(ts - TimeZone.getDefault.getOffset(ts))
  }

  def row(args: Any*):Row = {
    val row = new Row(args.length)
    0 until args.length foreach {
      i => row.setField(i, args(i))
    }
    row
  }

  @Test
  def testWindowAggregateOnRowtime(): Unit = {
    TestCollectionTableFactory.initData(data.asJava)
    val ddl =
      """
        |CREATE TABLE src (
        |  ts TIMESTAMP(3),
        |  a INT,
        |  b DOUBLE,
        |  c FLOAT,
        |  d DECIMAL,
        |  e STRING,
        |  WATERMARK FOR ts AS ts
        |) WITH (
        |  'connector' = 'COLLECTION'
        |)
      """.stripMargin
    val query =
      """
        |SELECT TUMBLE_END(ts, INTERVAL '0.003' SECOND), COUNT(ts)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '0.003' SECOND)
      """.stripMargin
    tEnv.sqlUpdate(ddl)
    val sink = new TestingAppendSink()
    tEnv.sqlQuery(query).toAppendStream[Row].addSink(sink)
    tEnv.execute("SQL JOB")

    val expected = Seq(
      "1970-01-01T00:00:00.003,2",
      "1970-01-01T00:00:00.006,2",
      "1970-01-01T00:00:00.009,2",
      "1970-01-01T00:00:00.018,1")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

}
