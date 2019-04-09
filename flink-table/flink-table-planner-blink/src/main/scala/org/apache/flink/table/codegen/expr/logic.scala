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

package org.apache.flink.table.codegen.expr

import org.apache.flink.table.`type`.{InternalType, InternalTypes}
import org.apache.flink.table.codegen.CodeGenUtils.{newNames, primitiveTypeTermForType, requireBoolean}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}

case class NotCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  requireBoolean(operand)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    // Three-valued logic:
    // no Unknown -> Two-valued logic
    // Unknown -> Unknown
    nullSafeCodeGen(ctx) {
      term => s"!($term)"
    }
  }
}


case class AndCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryExprCodeGen {

  requireBoolean(left)
  requireBoolean(right)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True && Unknown -> Unknown
      // False && Unknown -> False
      // Unknown && True -> Unknown
      // Unknown && False -> False
      // Unknown && Unknown -> Unknown
      s"""
         |${left.code}
         |
         |boolean $resultTerm = false;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && !${left.resultTerm}) {
         |  // left expr is false, skip right expr
         |} else {
         |  ${right.code}
         |
         |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} && ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
       """.stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = false;
         |if (${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, InternalTypes.BOOLEAN)
  }
}

case class OrCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryExprCodeGen {

  requireBoolean(left)
  requireBoolean(right)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True || Unknown -> True
      // False || Unknown -> Unknown
      // Unknown || True -> True
      // Unknown || False -> Unknown
      // Unknown || Unknown -> Unknown
      s"""
         |${left.code}
         |
        |boolean $resultTerm = true;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && ${left.resultTerm}) {
         |  // left expr is true, skip right expr
         |} else {
         |  ${right.code}
         |
        |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} || ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
         |""".stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = true;
         |if (!${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, InternalTypes.BOOLEAN)
  }
}

case class IfThenElseCodeGen(
    operands: Seq[GeneratedExpression],
    resultType: InternalType)
  extends ExprCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    generateIfThenElse(ctx, 0)
  }

  private def generateIfThenElse(
    ctx: CodeGeneratorContext,
    i: Int)
  : GeneratedExpression = {
    // else part
    if (i == operands.size - 1) {
      CastCodeGen(operands(i), resultType).codegen(ctx)
    }
    else {
      // check that the condition is boolean
      // we do not check for null instead we use the default value
      // thus null is false
      requireBoolean(operands(i))
      val condition = operands(i)
      val trueAction = CastCodeGen(operands(i), resultType).codegen(ctx)
      val falseAction = generateIfThenElse(ctx, i + 2)

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val resultTypeTerm = primitiveTypeTermForType(resultType)

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm;
           |boolean $nullTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $resultTerm = ${trueAction.resultTerm};
           |  $nullTerm = ${trueAction.nullTerm};
           |}
           |else {
           |  ${falseAction.code}
           |  $resultTerm = ${falseAction.resultTerm};
           |  $nullTerm = ${falseAction.nullTerm};
           |}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $resultTerm = ${trueAction.resultTerm};
           |}
           |else {
           |  ${falseAction.code}
           |  $resultTerm = ${falseAction.resultTerm};
           |}
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, resultType)
    }
  }
}
