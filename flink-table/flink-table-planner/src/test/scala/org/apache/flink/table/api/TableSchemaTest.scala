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

package org.apache.flink.table.api

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.expressions.utils.ApiExpressionUtils
import org.apache.flink.table.types.DataType
import org.apache.flink.table.utils.TableTestBase
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class TableSchemaTest extends TableTestBase {

  /** A watermark expression placeholder. */
  /** A watermark expression placeholder. */
  val WATERMARK_STRATEGY: Expression = ApiExpressionUtils.valueLiteral(0L)

  @Test
  def testBatchTableSchema(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getFieldNames.apply(0))
    assertEquals("b", schema.getFieldNames.apply(1))

    assertEquals(Types.INT, schema.getFieldTypes.apply(0))
    assertEquals(Types.STRING, schema.getFieldTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: INT\n" +
      " |-- b: STRING\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(!schema.getFieldName(3).isPresent)
    assertTrue(!schema.getFieldType(-1).isPresent)
    assertTrue(!schema.getFieldType("c").isPresent)
  }

  @Test
  def testStreamTableSchema(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, String)]("MyTable", 'a, 'b)
    val schema = table.getSchema

    assertEquals("a", schema.getFieldNames.apply(0))
    assertEquals("b", schema.getFieldNames.apply(1))

    assertEquals(Types.INT, schema.getFieldTypes.apply(0))
    assertEquals(Types.STRING, schema.getFieldTypes.apply(1))

    val expectedString = "root\n" +
      " |-- a: INT\n" +
      " |-- b: STRING\n"
    assertEquals(expectedString, schema.toString)

    assertTrue(!schema.getFieldName(3).isPresent)
    assertTrue(!schema.getFieldType(-1).isPresent)
    assertTrue(!schema.getFieldType("c").isPresent)
  }

  @Test
  def testSchemaWithWatermark(): Unit = {
    val data: Seq[(String, DataType, String)] = Seq(
      ("a", DataTypes.BIGINT(), "but is of type BIGINT"),
      ("b", DataTypes.STRING(), "but is of type STRING"),
      ("c", DataTypes.INT(), "but is of type STRING"),
      ("d", DataTypes.TIMESTAMP(), "PASS"),
      ("e", DataTypes.TIMESTAMP(0), "PASS"),
      ("f", DataTypes.TIMESTAMP(3), "PASS"),
      ("g", DataTypes.TIMESTAMP(9), "PASS")
    )

    data.foreach { case (fieldName, _, errorMsg) =>
      val builder = TableSchema.builder()
      data.foreach { case (name, t, _) => builder.field(name, t) }
      builder.watermark(fieldName, WATERMARK_STRATEGY)
      if (errorMsg.equals("PASS")) {
        val schema = builder.build()
        assertEquals(fieldName, schema.getRowtimeAttribute)
      } else {
        thrown.expectMessage(errorMsg)
        builder.build()
      }
    }
  }

  @Test
  def testSchemaWithNestedRowtime(): Unit = {
    val schema = TableSchema.builder()
      .field("f0", DataTypes.BIGINT())
      .field("f1", DataTypes.ROW(
        DataTypes.FIELD("q1", DataTypes.STRING()),
        DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("q3", DataTypes.ROW(
          DataTypes.FIELD("t1", DataTypes.TIMESTAMP(3)),
          DataTypes.FIELD("t2", DataTypes.STRING())
        )))
      )
      .watermark("f1.q3.t1", WATERMARK_STRATEGY)
      .build()

    assertTrue(schema.getRowtimeAttribute.isPresent)
    assertEquals("f1.q3.t1", schema.getRowtimeAttribute.get())
  }

  @Test
  def testSchemaWithNonExistedRowtime(): Unit = {
    thrown.expectMessage("Rowtime attribute 'f1.q0' is not defined in schema")

    TableSchema.builder()
      .field("f0", DataTypes.BIGINT())
      .field("f1", DataTypes.ROW(
        DataTypes.FIELD("q1", DataTypes.STRING()),
        DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3))))
      .watermark("f1.q0", WATERMARK_STRATEGY)
      .build()
  }

  @Test
  def testSchemaWithMultipleWatermark(): Unit = {
    thrown.expectMessage("Multiple watermark definition is not supported yet.")

    TableSchema.builder()
      .field("f0", DataTypes.TIMESTAMP())
      .field("f1", DataTypes.ROW(
        DataTypes.FIELD("q1", DataTypes.STRING()),
        DataTypes.FIELD("q2", DataTypes.TIMESTAMP(3))))
      .watermark("f1.q2", WATERMARK_STRATEGY)
      .watermark("f0", WATERMARK_STRATEGY)
      .build()
  }
}
