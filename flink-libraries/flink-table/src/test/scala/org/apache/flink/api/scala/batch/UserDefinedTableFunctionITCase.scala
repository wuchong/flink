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
package org.apache.flink.api.scala.batch

import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils.{TableFunc0, TableFunc2, TableFunc1}
import org.apache.flink.api.table.{Row, Table, TableEnvironment}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._
import scala.collection.mutable

@RunWith(classOf[Parameterized])
class UserDefinedTableFunctionITCase(
  mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  @Test
  def testUDTF(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", TableFunc1)

    val sqlQuery = "SELECT MyTable.c, t.s FROM MyTable CROSS APPLY split(c) AS t(s)"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack\n" + "Jack#22,22\n" + "John#19,John\n" + "John#19,19\n" +
      "Anna#44,Anna\n" + "Anna#44,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)

    // with overloading
    val sqlQuery2 = "SELECT MyTable.c, t.s FROM MyTable CROSS APPLY split(c, '$') AS t(s)"
    val result2 = tableEnv.sql(sqlQuery2).toDataSet[Row]
    val results2 = result2.collect()
    val expected2: String = "Jack#22,$Jack\n" + "Jack#22,$22\n" + "John#19,$John\n" +
      "John#19,$19\n" + "Anna#44,$Anna\n" + "Anna#44,$44\n"
    TestBaseUtils.compareResultAsText(results2.asJava, expected2)
  }

  @Test
  def testUDTFCustomReturnType(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", TableFunc2)

    val sqlQuery = "SELECT MyTable.c, t.a, t.b  FROM MyTable CROSS APPLY split(c) AS t(a,b)"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFWithOuterApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", TableFunc2)

    val sqlQuery = "SELECT MyTable.c, t.a, t.b  FROM MyTable OUTER APPLY split(c) AS t(a,b)"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFWithFilter(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)
    tableEnv.registerTable("MyTable", in)
    tableEnv.registerFunction("split", TableFunc0)

    val sqlQuery = "SELECT MyTable.c, t.name, t.age " +
      "FROM MyTable CROSS APPLY split(c) AS t(name,age) " +
      "WHERE t.age > 20"

    val result = tableEnv.sql(sqlQuery).toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFWithScalarTableAPI(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)

    val table = in.crossApply(TableFunc0('c) as ('name, 'age))
      .select('c, 'name, 'age)
      .where('age > 20)

    val result = table.toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,22\n" + "Anna#44,Anna,44\n"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testUDTFWithScalarTableAPIAndOuterApply(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
    val in: Table = getSmall3TupleDataSet(env).toTable(tableEnv).as('a, 'b, 'c)

    val table = in.outerApply(TableFunc2('c) as ('name, 'age))
      .select('c, 'name, 'age)

    val result = table.toDataSet[Row]
    val results = result.collect()
    val expected: String = "Jack#22,Jack,4\n" + "Jack#22,22,2\n" + "John#19,John,4\n" +
      "John#19,19,2\n" + "Anna#44,Anna,4\n" + "Anna#44,44,2\n" + "nosharp,null,null"
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  private def getSmall3TupleDataSet(env: ExecutionEnvironment): DataSet[(Int, Long, String)] = {
    val data = new mutable.MutableList[(Int, Long, String)]
    data.+=((1, 1L, "Jack#22"))
    data.+=((2, 2L, "John#19"))
    data.+=((3, 2L, "Anna#44"))
    data.+=((4, 3L, "nosharp"))
    env.fromCollection(data)
  }
}
