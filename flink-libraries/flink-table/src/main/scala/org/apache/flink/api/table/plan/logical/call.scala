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
package org.apache.flink.api.table.plan.logical

import java.lang.reflect.Method

import org.apache.calcite.rel.logical.LogicalTableFunctionScan
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.{FlinkTypeFactory, TableEnvironment, TableException, UnresolvedException}
import org.apache.flink.api.table.expressions.{Attribute, Expression, ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.api.table.functions.TableFunction
import org.apache.flink.api.table.functions.utils.TableSqlFunction
import org.apache.flink.api.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.api.table.plan.schema.FlinkTableFunctionImpl
import org.apache.flink.api.table.validate.ValidationFailure

import scala.collection.JavaConversions._

/**
  * General expression for unresolved user-defined table function calls.
  */
case class UnresolvedTableFunctionCall(functionName: String, args: Seq[Expression]) extends LogicalNode {

  override def output: Seq[Attribute] =
    throw UnresolvedException("Invalid call to output on UnresolvedTableFunctionCall")

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder =
    throw UnresolvedException("Invalid call to construct on UnresolvedTableFunctionCall")

  override private[flink] def children: Seq[LogicalNode] =
    throw UnresolvedException("Invalid call to children on UnresolvedTableFunctionCall")
}

/**
  * LogicalNode for calling a user-defined table functions.
  * @param tableFunction table function to be called (might be overloaded)
  * @param parameters actual parameters
  * @param alias output fields renaming
  * @tparam T type of returned table
  */
case class TableFunctionCall[T: TypeInformation](
  tableFunction: TableFunction[T],
  parameters: Seq[Expression],
  alias: Option[Array[String]]) extends UnaryNode {

  private var table: LogicalNode = _
  override def child: LogicalNode = table

  def setChild(child: LogicalNode): TableFunctionCall[T] = {
    table = child
    this
  }

  private val resultType: TypeInformation[T] =
    if (tableFunction.getResultType == null) {
      implicitly[TypeInformation[T]]
    } else {
      tableFunction.getResultType
    }

  private val fieldNames: Array[String] =
    if (alias.isEmpty) {
      getFieldAttribute[T](resultType)._1
    } else {
      alias.get
    }
  private val fieldTypes: Array[TypeInformation[_]] = getFieldAttribute[T](resultType)._2

  /**
    * Assigns an alias for this table function returned fields that the following `select()` clause
    * can refer to.
    *
    * @param aliasList alias for this window
    * @return this table function
    */
  def as(aliasList: Expression*): TableFunctionCall[T] = {
    if (aliasList == null) {
      return this
    }
    if (aliasList.length != fieldNames.length) {
      failValidation("Aliasing not match number of fields")
    } else if (!aliasList.forall(_.isInstanceOf[UnresolvedFieldReference])) {
      failValidation("Alias only accept name expressions as arguments")
    } else {
      val names = aliasList.map(_.asInstanceOf[UnresolvedFieldReference].name).toArray
      TableFunctionCall(tableFunction, parameters, Some(names))
    }
  }

  override def output: Seq[Attribute] = fieldNames.zip(fieldTypes).map {
    case (n, t) =>  ResolvedFieldReference(n, t)
  }

  override def makeCopy(newArgs: Array[AnyRef]): TableFunctionCall[T] = {
    if (newArgs.length != 3) {
      throw new TableException("Invalid constructor params")
    }
    val udtfParam: TableFunction[T] = newArgs.head.asInstanceOf[TableFunction[T]]
    val expressionParams = newArgs(1).asInstanceOf[Seq[Expression]]
    val names = newArgs.last.asInstanceOf[Option[Array[String]]]
    copy(udtfParam, expressionParams, names)
      .asInstanceOf[TableFunctionCall[T]].setChild(child)
  }

  private var evalMethod: Method = _

  override def validate(tableEnv: TableEnvironment): LogicalNode = {
    val node = super.validate(tableEnv).asInstanceOf[TableFunctionCall[T]]
    val signature = node.parameters.map(_.resultType)
    // look for a signature that matches the input types
    val foundMethod = getEvalMethod(tableFunction, signature)
    if (foundMethod.isEmpty) {
      ValidationFailure(s"Given parameters do not match any signature. \n" +
                          s"Actual: ${signatureToString(signature)} \n" +
                          s"Expected: ${signaturesToString(tableFunction)}")
    } else {
      node.evalMethod = foundMethod.get
    }
    node
  }

  override protected[logical] def construct(relBuilder: RelBuilder): RelBuilder = {
    val fieldIndexes = getFieldInfo(resultType)._2
    val function = new FlinkTableFunctionImpl(resultType, fieldIndexes, fieldNames, evalMethod)
    val typeFactory = relBuilder.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val sqlFunction = TableSqlFunction(tableFunction.toString, tableFunction,
                                       resultType, typeFactory, function)

    val scan = LogicalTableFunctionScan.create(
      relBuilder.peek().getCluster,
      List(),
      relBuilder.call(sqlFunction, parameters.map(_.toRexNode(relBuilder))),
      function.getElementType(null),
      function.getRowType(relBuilder.getTypeFactory, null),
      null)
    relBuilder.push(scan)
  }
}



case class TableFunctionCallBuilder[T: TypeInformation](udtf: TableFunction[T]) {
  /**
    * Creates a call to a [[TableFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[LogicalNode]] in form of a [[TableFunctionCall]]
    */
  def apply(params: Expression*): TableFunctionCall[T] = {
    TableFunctionCall(udtf, params.toSeq, None)
  }
}
