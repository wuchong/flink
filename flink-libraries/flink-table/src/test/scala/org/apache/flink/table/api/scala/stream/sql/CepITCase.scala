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
package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class CepITCase extends StreamingWithStateTestBase {


  @Test
  def testSimpleCEP() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(Int, String)]
    data.+=((1, "a"))
    data.+=((2, "z"))
    data.+=((3, "b"))
    data.+=((4, "c"))
    data.+=((5, "d"))
    data.+=((6, "a"))
    data.+=((7, "b"))
    data.+=((8, "c"))
    data.+=((9, "h"))

    val t = env.fromCollection(data).toTable(tEnv).as('id, 'name)
    tEnv.registerTable("MyTable", t)

    val sqlQuery =
      s"""
         |SELECT * FROM MyTable MATCH_RECOGNIZE
         |(
         |  PATTERN (A B C)
         |  DEFINE
         |    A AS A.name = 'a',
         |    B AS B.name = 'b',
         |    C AS C.name = 'c'
         |) MR
       """.stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    // Calcite doesn’t support MEASURES yet, as a workaround,
    // we hardcode to only output the last event in the matched events
    val expected = List("8,c")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleCEP2() = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val data = new mutable.MutableList[(String, Long, Int)]
    data.+=(("ACME", 1L, 12))
    data.+=(("ACME", 2L, 17))
    data.+=(("ACME", 3L, 13))
    data.+=(("ACME", 4L, 15))
    data.+=(("ACME", 5L, 20))
    data.+=(("ACME", 6L, 24))
    data.+=(("ACME", 7L, 25))
    data.+=(("ACME", 8L, 19))

    val t = env.fromCollection(data).toTable(tEnv).as('symbol, 'tstamp, 'price)
    tEnv.registerTable("Ticker", t)

    val sqlQuery =
      s"""
         |SELECT tstamp, price FROM Ticker MATCH_RECOGNIZE
         |(
         |  PATTERN (STRT DOWN+ UP+)
         |  DEFINE
         |    DOWN AS DOWN.price < PREV(DOWN.price),
         |    UP AS UP.price > PREV(UP.price)
         |) MR
       """.stripMargin

    val result = tEnv.sql(sqlQuery).toAppendStream[Row]
    result.addSink(new StreamITCase.StringSink)
    env.execute()

    // Calcite doesn’t support MEASURES yet, as a workaround,
    // we hardcode to only output the last event in the matched events
    val expected = List("4,15", "5,20", "6,24", "7,25")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
