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
package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils.{getAccumulatorTypeOfAggregateFunction, getResultTypeOfAggregateFunction}
import org.apache.flink.table.functions.{AggregateFunction, MultisetAggregateFunction, OperatorFunction}
import org.apache.flink.table.typeutils.MultisetTypeInfo

/**
  * A class which creates a call to an aggregateFunction
  */
case class UDAGGExpression[T: TypeInformation, ACC: TypeInformation](
  aggregateFunction: AggregateFunction[T, ACC]) {

  /**
    * Creates a call to an [[AggregateFunction]].
    *
    * @param params actual parameters of function
    * @return a [[AggFunctionCall]]
    */
  def apply(params: Expression*): AggFunctionCall = {
    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      aggregateFunction,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      aggregateFunction,
      implicitly[TypeInformation[ACC]])

    AggFunctionCall(aggregateFunction, resultTypeInfo, accTypeInfo, params)
  }
}

case class MultisetUDAGGExpression[T: TypeInformation, ACC: TypeInformation](
    aggregateFunction: MultisetAggregateFunction[T, ACC]) {

  /**
    * Creates a call to an [[AggregateFunction]].
    *
    * @param params actual parameters of function
    * @return a [[AggFunctionCall]]
    */
  def apply(params: Expression*): AggFunctionCall = {
    val resultTypeInfo: TypeInformation[_] = getResultTypeOfAggregateFunction(
      aggregateFunction,
      implicitly[TypeInformation[T]])

    val accTypeInfo: TypeInformation[_] = getAccumulatorTypeOfAggregateFunction(
      aggregateFunction,
      implicitly[TypeInformation[ACC]])

    val multsetTypeInfo = new MultisetTypeInfo(resultTypeInfo)

    AggFunctionCall(aggregateFunction, multsetTypeInfo, accTypeInfo, params)
  }
}


///**
//  * A class which creates a call to an aggregateFunction
//  */
//case class UDOPExpression[T: TypeInformation, ACC](operatorFunction: OperatorFunction[T, ACC]) {
//
//  /**
//    * Creates a call to an [[AggregateFunction]].
//    *
//    * @param params actual parameters of function
//    * @return a [[AggFunctionCall]]
//    */
//  def apply(params: Expression*): OperatorFunctionCall =
//    OperatorFunctionCall(operatorFunction, params)
//}
