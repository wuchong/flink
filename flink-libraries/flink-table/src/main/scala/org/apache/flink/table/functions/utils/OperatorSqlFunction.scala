///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.table.functions.utils
//
//import org.apache.calcite.rel.`type`.RelDataType
//import org.apache.calcite.sql._
//import org.apache.calcite.sql.`type`.SqlReturnTypeInference
//import org.apache.calcite.sql.parser.SqlParserPos
//import org.apache.calcite.sql.validate.SqlUserDefinedAggFunction
//import org.apache.flink.api.common.typeinfo._
//import org.apache.flink.table.calcite.FlinkTypeFactory
//import org.apache.flink.table.functions.OperatorFunction
//import org.apache.flink.table.functions.utils.AggSqlFunction.{createOperandTypeChecker, createOperandTypeInference}
//import org.apache.flink.table.functions.utils.OperatorSqlFunction.createReturnTypeInference
//
///**
//  * Calcite wrapper for user-defined aggregate functions.
//  *
//  * @param name function name (used by SQL parser)
//  * @param returnType the type information of returned value
//  * @param typeFactory type factory for converting Flink's between Calcite's types
//  */
//class OperatorSqlFunction(
//    name: String,
//    operatorFun: OperatorFunction[_, _],
//    returnType: TypeInformation[_],
//    typeFactory: FlinkTypeFactory)
//  extends SqlUserDefinedAggFunction(
//    new SqlIdentifier(name, SqlParserPos.ZERO),
//    createReturnTypeInference(returnType, typeFactory),
//    createOperandTypeInference(operatorFun, typeFactory),
//    createOperandTypeChecker(operatorFun),
//    null) {
//
//  def getFunction: OperatorFunction[_, _] = operatorFun
//}
//
//object OperatorSqlFunction {
//
//  def apply(
//    name: String,
//    operatorFun: OperatorFunction[_, _],
//    returnType: TypeInformation[_],
//    typeFactory: FlinkTypeFactory): OperatorSqlFunction = {
//    new OperatorSqlFunction(name, operatorFun, returnType, typeFactory)
//  }
//
//  private[flink] def createReturnTypeInference(
//    resultType: TypeInformation[_],
//    typeFactory: FlinkTypeFactory)
//  : SqlReturnTypeInference = {
//
//    new SqlReturnTypeInference {
//      override def inferReturnType(opBinding: SqlOperatorBinding): RelDataType = {
//        val elementType = typeFactory.createTypeFromTypeInfo(resultType)
//        typeFactory.createMultisetType(elementType, -1)
//      }
//    }
//  }
//}
//
//
