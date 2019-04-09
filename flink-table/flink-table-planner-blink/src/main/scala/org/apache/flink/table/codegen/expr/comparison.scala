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
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.GeneratedExpression.NEVER_NULL
import org.apache.flink.table.codegen.calls.ScalarOperatorGens.stringToInternalCode
import org.apache.flink.table.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.dataformat.Decimal
import org.apache.flink.table.typeutils.TypeCheckUtils._
import org.apache.flink.util.Preconditions.checkArgument

abstract class BinaryComparisonCodeGen extends BinaryExprCodeGen {

  override def resultType: InternalType = InternalTypes.BOOLEAN

  /**
    * Generates comparison code for numeric types and comparable types of same type.
    */
  protected def codegenComparison(
      ctx: CodeGeneratorContext,
      operator: String): GeneratedExpression = {
    nullSafeCodeGen(ctx) {
      // either side is decimal
      if (isDecimal(left.resultType) || isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => {
          s"${className[Decimal]}.compare($leftTerm, $rightTerm) $operator 0"
        }
      }
      // both sides are numeric
      else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      // both sides are temporal of same type
      else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      // both sides are boolean
      else if (isBoolean(left.resultType) && left.resultType == right.resultType) {
        operator match {
          case "==" | "!=" => (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
          case ">" | "<" | "<=" | ">=" =>
            (leftTerm, rightTerm) =>
              s"java.lang.Boolean.compare($leftTerm, $rightTerm) $operator 0"
          case _ => throw new CodeGenException(s"Unsupported boolean comparison '$operator'.")
        }
      }
      // both sides are binary type
      else if (isBinary(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) =>
          s"java.util.Arrays.equals($leftTerm, $rightTerm)"
      }
      // both sides are same comparable type
      else if (isComparable(left.resultType) && left.resultType == right.resultType) {
        (leftTerm, rightTerm) =>
          s"(($leftTerm == null) ? (($rightTerm == null) ? 0 : -1) : (($rightTerm == null) ? " +
            s"1 : ($leftTerm.compareTo($rightTerm)))) $operator 0"
      }
      else {
        throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
          s"${right.resultType}")
      }
    }
  }
}

case class EqualsCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (left.resultType == InternalTypes.STRING && right.resultType == InternalTypes.STRING) {
      nullSafeCodeGen(ctx) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      codegenComparison(ctx, "==")
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      codegenComparison(ctx, "==")
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      // BinaryArray.equals
      nullSafeCodeGen(ctx) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      // BinaryMap.equals
      nullSafeCodeGen(ctx) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      codegenComparison(ctx, "==")
    }
    // support date/time/timestamp equalTo string.
    // for performance, we cast literal string to literal time.
    else if (isTimePoint(left.resultType) && right.resultType == InternalTypes.STRING) {
      if (right.literal) {
        EqualsCodeGen(left, castStringLiteralToDateTime(ctx, right, left.resultType))
          .codegen(ctx)
      } else {
        EqualsCodeGen(left, CastCodeGen(right, left.resultType).codegen(ctx))
          .codegen(ctx)
      }
    }
    else if (isTimePoint(right.resultType) && left.resultType == InternalTypes.STRING) {
      if (left.literal) {
        EqualsCodeGen(castStringLiteralToDateTime(ctx, left, right.resultType), right)
          .codegen(ctx)
      } else {
        EqualsCodeGen(CastCodeGen(left, right.resultType).codegen(ctx), right)
          .codegen(ctx)
      }
    }
    // non comparable types
    else {
      nullSafeCodeGen(ctx) {
        if (isReference(left)) {
          (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
        }
        else if (isReference(right)) {
          (leftTerm, rightTerm) => s"$rightTerm.equals($leftTerm)"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }

  private def castStringLiteralToDateTime(
      ctx: CodeGeneratorContext,
      stringLiteral: GeneratedExpression,
      expectType: InternalType): GeneratedExpression = {
    checkArgument(stringLiteral.literal)
    val rightTerm = stringLiteral.resultTerm
    val typeTerm = primitiveTypeTermForType(expectType)
    val defaultTerm = primitiveDefaultValue(expectType)
    val term = newName("stringToTime")
    val zoneTerm = ctx.addReusableTimeZone()
    val code = stringToInternalCode(expectType, rightTerm, zoneTerm)
    val stmt = s"$typeTerm $term = ${stringLiteral.nullTerm} ? $defaultTerm : $code;"
    ctx.addReusableMember(stmt)
    stringLiteral.copy(resultType = expectType, resultTerm = term)
  }
}

case class NotEqualsCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (left.resultType == InternalTypes.STRING && right.resultType == InternalTypes.STRING) {
      nullSafeCodeGen(ctx) {
        (leftTerm, rightTerm) => s"!$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      codegenComparison(ctx, "!=")
    }
    // temporal types
    else if (isTemporal(left.resultType) && left.resultType == right.resultType) {
      codegenComparison(ctx, "!=")
    }
    // array types
    else if (isArray(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = EqualsCodeGen(left, right).codegen(ctx)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})",
        equalsExpr.nullTerm,
        equalsExpr.code,
        InternalTypes.BOOLEAN)
    }
    // map types
    else if (isMap(left.resultType) && left.resultType == right.resultType) {
      val equalsExpr = EqualsCodeGen(left, right).codegen(ctx)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})",
        equalsExpr.nullTerm,
        equalsExpr.code,
        InternalTypes.BOOLEAN)
    }
    // comparable types
    else if (isComparable(left.resultType) && left.resultType == right.resultType) {
      codegenComparison(ctx, "!=")
    }
    // non-comparable types
    else {
      nullSafeCodeGen(ctx) {
        if (isReference(left)) {
          (leftTerm, rightTerm) => s"!($leftTerm.equals($rightTerm))"
        }
        else if (isReference(right)) {
          (leftTerm, rightTerm) => s"!($rightTerm.equals($leftTerm))"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }
}

case class GreaterThanCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  requireComparable(left)
  requireComparable(right)

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    codegenComparison(ctx, ">")
  }
}


case class GreaterThanOrEqualCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  requireComparable(left)
  requireComparable(right)

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    codegenComparison(ctx, ">=")
  }
}


case class LessThanCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  requireComparable(left)
  requireComparable(right)

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    codegenComparison(ctx, "<")
  }
}

case class LessThanOrEqualCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression)
  extends BinaryComparisonCodeGen {

  requireComparable(left)
  requireComparable(right)

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    codegenComparison(ctx, "<=")
  }
}

case class IsNullCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (ctx.nullCheck) {
      GeneratedExpression(operand.nullTerm, NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
    else if (!ctx.nullCheck && isReference(operand)) {
      val resultTerm = newName("isNull")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} == null;
           |""".stripMargin
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("false", NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
  }
}

case class IsNotNullCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (ctx.nullCheck) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = !${operand.nullTerm};
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else if (!ctx.nullCheck && isReference(operand)) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} != null;
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, InternalTypes.BOOLEAN)
    }
    else {
      GeneratedExpression("true", NEVER_NULL, operand.code, InternalTypes.BOOLEAN)
    }
  }
}

case class IsTrueCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  requireBoolean(operand)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    GeneratedExpression(
      operand.resultTerm, // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }
}

case class IsFalseCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  requireBoolean(operand)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm} && !${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }
}

case class IsNotTrueCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  requireBoolean(operand)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm})", // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }
}

case class IsNotFalseCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {

  requireBoolean(operand)

  override def resultType: InternalType = InternalTypes.BOOLEAN

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    GeneratedExpression(
      s"(${operand.resultTerm} || ${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      InternalTypes.BOOLEAN)
  }
}
