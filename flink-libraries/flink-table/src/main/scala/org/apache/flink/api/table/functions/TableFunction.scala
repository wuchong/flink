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

package org.apache.flink.api.table.functions

import org.apache.calcite.sql.SqlFunction
import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.table.{ValidationException, FlinkTypeFactory}

import scala.collection.mutable.ListBuffer

/**
  * Base class for a user-defined table function (UDTF). A user-defined table functions works on
  * one row as input and returns multiple rows as output.
  *
  * The behavior of a [[TableFunction]] can be defined by implementing a custom evaluation
  * method. An evaluation method must be declared publicly and named "eval". Evaluation methods
  * can also be overloaded by implementing multiple methods named "eval".
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  *
  * By default the result type of an evaluation method is determined by Flink's type extraction
  * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
  * complex, custom, or composite types. In these cases [[TypeInformation]] of the result type
  * can be manually defined by overriding [[getResultType()]].
  *
  * Internally, the Table/SQL API code generation works with primitive values as much as possible.
  * If a user-defined table function should not introduce much overhead during runtime, it is
  * recommended to declare parameters and result types as primitive types instead of their boxed
  * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
  *
  * @tparam T The type of the output row
  */
abstract class TableFunction[T] extends UserDefinedFunction with EvaluableFunction {

  private val rows: ListBuffer[T] = new ListBuffer

  /**
    * Emit an output row
    *
    * @param row the output row
    */
  protected def collect(row: T): Unit = {
    // cache rows for now, maybe immediately process them further
    rows += row
  }


  @Internal
  def getRowsIterator = rows.toIterator

  @Internal
  def clear() = rows.clear()

  // this method will not be called, because we need to register multiple sql function at one time
  override private[flink] final def createSqlFunction(
      name: String,
      typeFactory: FlinkTypeFactory)
    : SqlFunction = {
    null
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the result type of the evaluation method with a given signature.
    *
    * This method needs to be overriden in case Flink's type extraction facilities are not
    * sufficient to extract the [[TypeInformation]] based on the return type of the evaluation
    * method. Flink's type extraction facilities can handle basic types or
    * simple POJOs but might be wrong for more complex, custom, or composite types.
    *
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType: TypeInformation[T] = null

  /**
    * Returns [[TypeInformation]] about the operands of the evaluation method with a given
    * signature.
    *
    * In order to perform operand type inference in SQL (especially when NULL is used) it might be
    * necessary to determine the parameter [[TypeInformation]] of an evaluation method.
    * By default Flink's type extraction facilities are used for this but might be wrong for
    * more complex, custom, or composite types.
    *
    * @param signature signature of the method the operand types need to be determined
    * @return [[TypeInformation]] of  operand types
    */
  def getParameterTypes(signature: Array[Class[_]]): Array[TypeInformation[_]] = {
    signature.map { c =>
      try {
        TypeExtractor.getForClass(c)
      } catch {
        case ite: InvalidTypesException =>
          throw new ValidationException(
            s"Parameter types of scalar function '$this' cannot be " +
              s"automatically determined. Please provide type information manually.")
      }
    }
  }
}
