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
package org.apache.flink.table.codegen

import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.rex.{RexCall, RexInputRef, RexLiteral}
import org.apache.calcite.sql.fun.SqlStdOperatorTable.PREV
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.Indenter.toISC
import org.apache.flink.table.codegen.CodeGenUtils.{boxedTypeTermForTypeInfo, newName, primitiveDefaultValue, primitiveTypeTermForTypeInfo}
import org.apache.flink.types.Row

/**
  * A code generator for generating Flink CEP relative
  * [[org.apache.flink.api.common.functions.Function]]s.
  *
  * @param config configuration that determines runtime behavior
  * @param input type information about the first input of the Function
  * @param patternName the name of current pattern
  * @param previousPatternName the name of previous pattern
  */
class MatchCodeGenerator(
    config: TableConfig,
    input: TypeInformation[_ <: Any],
    patternName: String,
    previousPatternName: String)
  extends CodeGenerator(config, false, input){

  /**
    * Generates a [[IterativeCondition]] that can be passed to Java compiler.
    *
    * @param name Class name of the iterative condition. Must not be unique but has to be a
    *             valid Java class identifier.
    * @param bodyCode body code for the iterative condition method
    * @return instance of GeneratedIterativeCondition
    */
  def generateIterativeCondition(
      name: String,
      bodyCode: String): GeneratedIterativeCondition = {

    val funcName = newName(name)
    val inputTypeTerm = boxedTypeTermForTypeInfo(input)

    val funcCode =j"""
      public class $funcName
          extends ${classOf[IterativeCondition[_]].getCanonicalName} {

        ${reuseMemberCode()}

        public $funcName() throws Exception {
          ${reuseInitCode()}
        }

        @Override
      	public boolean filter(Object _in1, ${classOf[IterativeCondition.Context[_]]} $contextTerm)
      	  throws Exception {

          $inputTypeTerm $input1Term = ($inputTypeTerm) _in1;
          ${reusePerRecordCode()}
          ${reuseInputUnboxingCode()}
          $bodyCode
      	}
      }
    """.stripMargin

    GeneratedIterativeCondition(funcName, funcCode)
  }


  override def visitCall(call: RexCall): GeneratedExpression = {
    val resultType = FlinkTypeFactory.toTypeInfo(call.getType)
    call.getOperator match {
      case PREV =>
        generatePrevious(
          call.operands.get(0).asInstanceOf[RexInputRef],
          call.operands.get(1).asInstanceOf[RexLiteral],
          resultType)

      // TODO: support LAST, NEXT, FIRST, RUNNING, FINAL in the future

      case _ => super.visitCall(call)
    }
  }

  private def generatePrevious(
      inputRef: RexInputRef,
      countLiteral: RexLiteral,
      resultType: TypeInformation[_]): GeneratedExpression = {

    val count = countLiteral.getValue3.asInstanceOf[JBigDecimal].intValue()


    if (count == 0) {
      // return current one
      visitInputRef(inputRef)
    } else {

      val listName = newName("patternEvents")
      val previousListName = newName("previousPatternEvents")
      val resultTerm = newName("result")
      val nullTerm = newName("isNull")
      val indexTerm = newName("eventIndex")
      val resultTypeTerm = boxedTypeTermForTypeInfo(resultType)
      val defaultValue = primitiveDefaultValue(resultType)

      val rowTypeTerm = classOf[Row].getCanonicalName
      val resultCode =
        s"""
           |java.util.List $listName = new java.util.ArrayList();
           |for ($rowTypeTerm event : $contextTerm.getEventsForPattern("$patternName")) {
           |    $listName.add(event.getField(${inputRef.getIndex}));
           |}
           |$resultTypeTerm $resultTerm;
           |boolean $nullTerm;
           |if ($listName.size() >= $count) {
           |  $resultTerm = ($resultTypeTerm) $listName.get($listName.size() - $count);
           |  $nullTerm = false;
           |} else {
           |  java.util.List $previousListName = new java.util.ArrayList();
           |  for ($rowTypeTerm event : $contextTerm.getEventsForPattern("$previousPatternName")) {
           |    $previousListName.add(event.getField(${inputRef.getIndex}));
           |  }
           |  int $indexTerm = $previousListName.size() - ($count - $listName.size());
           |  if ($indexTerm >= 0) {
           |    $resultTerm = ($resultTypeTerm) $previousListName.get($indexTerm);
           |    $nullTerm = false;
           |  } else {
           |    $resultTerm = $defaultValue;
           |    $nullTerm = true;
           |  }
           |}
     """.stripMargin

      GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)
    }
  }


}
