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

import org.apache.flink.table.`type`.InternalType
import org.apache.flink.table.codegen.CodeGenUtils.{primitiveDefaultValue, primitiveTypeTermForType}
import org.apache.flink.table.codegen.{CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.typeutils.TypeCheckUtils.{isReference, isTemporal}

trait ExprCodeGen {

  def operands: Seq[GeneratedExpression]

  def resultType: InternalType

  def codegen(ctx: CodeGeneratorContext): GeneratedExpression

  protected def nullSafeCodeGen(ctx: CodeGeneratorContext)
      (call: Seq[String] => String): GeneratedExpression = {
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val nullTerm = ctx.addReusableLocalVariable("boolean", "isNull")
    val resultTerm = ctx.addReusableLocalVariable(resultTypeTerm, "result")
    val defaultValue = primitiveDefaultValue(resultType)
    val resultNullable = isReference(resultType) && !isTemporal(resultType)
    val nullTermCode = if (ctx.nullCheck && resultNullable) {
      s"$nullTerm = ($resultTerm == null);"
    } else {
      ""
    }

    val result = call(operands.map(_.resultTerm))
    val operandsNullable = operands.nonEmpty && operands.forall(_.nullTerm != NEVER_NULL)

    val resultCode = if (ctx.nullCheck && operandsNullable) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = ${operands.map(_.nullTerm).mkString(" || ")};
         |$resultTerm = $defaultValue;
         |if (!$nullTerm) {
         |  $resultTerm = $result;
         |  $nullTermCode
         |}
         |""".stripMargin
    } else if (ctx.nullCheck && !operandsNullable) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$nullTerm = false;
         |$resultTerm = $result;
         |$nullTermCode
         |""".stripMargin
    } else {
      s"""
         |$nullTerm = false;
         |${operands.map(_.code).mkString("\n")}
         |$resultTerm = $result;
         |""".stripMargin
    }

    GeneratedExpression(resultTerm, nullTerm, resultCode, resultType)
  }
}

abstract class BinaryExprCodeGen extends ExprCodeGen {

  def left: GeneratedExpression

  def right: GeneratedExpression

  override def operands: Seq[GeneratedExpression] = Seq(left, right)

  protected def nullSafeCodeGen(ctx: CodeGeneratorContext)
    (call: (String, String) => String): GeneratedExpression = {
      super.nullSafeCodeGen(ctx) { args => call(args.head, args(1)) }
  }
}

abstract class UnaryExprCodeGen extends ExprCodeGen {

  def operand: GeneratedExpression

  override def operands: Seq[GeneratedExpression] = Seq(operand)

  protected def nullSafeCodeGen(ctx: CodeGeneratorContext)
      (call: String => String): GeneratedExpression = {
    super.nullSafeCodeGen(ctx) { args => call(args.head)}
  }
}

abstract class LeafExprCodeGen extends ExprCodeGen {

  override def operands: Seq[GeneratedExpression] = Seq()

  protected def nullSafeCodeGen(ctx: CodeGeneratorContext)
    (call: Unit => String): GeneratedExpression = {
    super.nullSafeCodeGen(ctx) { _ => call() }
  }
}
