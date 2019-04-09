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

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.util.BuiltInMethod
import org.apache.flink.table.`type`.{DecimalType, InternalType, InternalTypes}
import org.apache.flink.table.codegen.CodeGenUtils._
import org.apache.flink.table.codegen.calls.ScalarOperatorGens.numericCasting
import org.apache.flink.table.codegen.{CodeGenException, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.typeutils.TypeCheckUtils.{isDecimal, isNumeric, isTemporal, isTimeInterval}


abstract class BinaryArithmeticCodeGen extends BinaryExprCodeGen {

  requireNumeric(left)
  requireNumeric(right)

  protected def codegenBinaryArithmetic(
      ctx: CodeGeneratorContext,
      operator: String): GeneratedExpression = {
    resultType match {
      case dt: DecimalType =>
        // do not cast a decimal operand to resultType, which may change its value.
        // use it as is during calculation.
        def castToDec(t: InternalType): String => String = t match {
          case _: DecimalType => (operandTerm: String) => s"$operandTerm"
          case _ => numericCasting(t, resultType)
        }
        val methods = Map(
          "+" -> "add",
          "-" -> "subtract",
          "*" -> "multiply",
          "/" -> "divide",
          "%" -> "mod")

        nullSafeCodeGen(ctx) {
          (leftTerm, rightTerm) => {
            val method = methods(operator)
            val leftCasted = castToDec(left.resultType)(leftTerm)
            val rightCasted = castToDec(right.resultType)(rightTerm)
            val precision = dt.precision()
            val scale = dt.scale()
            s"$DECIMAL.$method($leftCasted, $rightCasted, $precision, $scale)"
          }
        }

      case _ =>
        val leftCasting = operator match {
          case "%" =>
            if (left.resultType == right.resultType) {
              numericCasting(left.resultType, resultType)
            } else {
              val castedType = if (isDecimal(left.resultType)) {
                InternalTypes.LONG
              } else {
                left.resultType
              }
              numericCasting(left.resultType, castedType)
            }
          case _ => numericCasting(left.resultType, resultType)
        }

        val rightCasting = numericCasting(right.resultType, resultType)
        val resultTypeTerm = primitiveTypeTermForType(resultType)

        nullSafeCodeGen(ctx) {
          (leftTerm, rightTerm) =>
            s"($resultTypeTerm) (${leftCasting(leftTerm)} $operator ${rightCasting(rightTerm)})"
        }
    }
  }

  protected def codegenBinaryTemporal(
      ctx: CodeGeneratorContext,
      operator: String): GeneratedExpression = {
    // only support plus or minus for temporal
    require(operator == "+" || operator == "-")

    (left.resultType, right.resultType) match {
      // arithmetic of time point and time interval
      case (InternalTypes.INTERVAL_MONTHS, InternalTypes.INTERVAL_MONTHS) |
           (InternalTypes.INTERVAL_MILLIS, InternalTypes.INTERVAL_MILLIS) =>
        codegenBinaryArithmetic(ctx, operator)

      case (InternalTypes.DATE, InternalTypes.INTERVAL_MILLIS) =>
        nullSafeCodeGen(ctx) {
          (l, r) => s"$l $operator ((int) ($r / ${MILLIS_PER_DAY}L))"
        }

      case (InternalTypes.DATE, InternalTypes.INTERVAL_MONTHS) =>
        nullSafeCodeGen(ctx) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $operator($r))"
        }

      case (InternalTypes.TIME, InternalTypes.INTERVAL_MILLIS) =>
        nullSafeCodeGen(ctx) {
          (l, r) => s"$l $operator ((int) ($r))"
        }

      case (InternalTypes.TIMESTAMP, InternalTypes.INTERVAL_MILLIS) =>
        nullSafeCodeGen(ctx) {
          (l, r) => s"$l $operator $r"
        }

      case (InternalTypes.TIMESTAMP, InternalTypes.INTERVAL_MONTHS) =>
        nullSafeCodeGen(ctx) {
          (l, r) => s"${qualifyMethod(BuiltInMethod.ADD_MONTHS.method)}($l, $operator($r))"
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF)
      case (InternalTypes.TIMESTAMP | InternalTypes.TIME | InternalTypes.DATE,
      InternalTypes.TIMESTAMP | InternalTypes.TIME | InternalTypes.DATE) if operator == "-" =>
        resultType match {
          case InternalTypes.INTERVAL_MONTHS =>
            nullSafeCodeGen(ctx) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (InternalTypes.TIMESTAMP, InternalTypes.DATE) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll, $rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.DATE, InternalTypes.TIMESTAMP) =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}" +
                    s"($ll * ${MILLIS_PER_DAY}L, $rr)"
                case _ =>
                  s"${qualifyMethod(BuiltInMethod.SUBTRACT_MONTHS.method)}($ll, $rr)"
              }
            }

          case InternalTypes.INTERVAL_MILLIS =>
            nullSafeCodeGen(ctx) {
              (ll, rr) => (left.resultType, right.resultType) match {
                case (InternalTypes.TIMESTAMP, InternalTypes.TIMESTAMP) =>
                  s"$ll $operator $rr"
                case (InternalTypes.DATE, InternalTypes.DATE) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $operator ($rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.TIMESTAMP, InternalTypes.DATE) =>
                  s"$ll $operator ($rr * ${MILLIS_PER_DAY}L)"
                case (InternalTypes.DATE, InternalTypes.TIMESTAMP) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $operator $rr"
              }
            }
        }

      case _ =>
        throw new CodeGenException("Unsupported temporal arithmetic.")
    }
  }
}


case class PlusCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression,
    resultType: InternalType)
  extends BinaryArithmeticCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (isNumeric(resultType)) {
      requireNumeric(left)
      requireNumeric(right)
      codegenBinaryArithmetic(ctx, "+")
    } else if (isTemporal(resultType)) {
      requireTemporal(left)
      requireTemporal(right)
      codegenBinaryTemporal(ctx, "+")
    } else {
      throw new CodeGenException("Unsupported arithmetic.")
    }
  }
}

case class MinusCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression,
    resultType: InternalType)
  extends BinaryArithmeticCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (isNumeric(resultType)) {
      requireNumeric(left)
      requireNumeric(right)
      codegenBinaryArithmetic(ctx, "-")
    } else if (isTemporal(resultType)) {
      requireTemporal(left)
      requireTemporal(right)
      codegenBinaryTemporal(ctx, "-")
    } else {
      throw new CodeGenException("Unsupported arithmetic.")
    }
  }
}

case class MultiplyCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression,
    resultType: InternalType)
  extends BinaryArithmeticCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (isNumeric(resultType)) {
      requireNumeric(left)
      requireNumeric(right)
      codegenBinaryArithmetic(ctx, "*")
    } else if (isTimeInterval(resultType)) {
      requireTimeInterval(left)
      requireNumeric(right)
      codegenBinaryTemporal(ctx, "*")
    } else {
      throw new CodeGenException("Unsupported arithmetic.")
    }
  }
}

case class DivideCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression,
    resultType: InternalType)
  extends BinaryArithmeticCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (isNumeric(resultType)) {
      requireNumeric(left)
      requireNumeric(right)
      codegenBinaryArithmetic(ctx, "/")
    } else {
      throw new CodeGenException("Unsupported arithmetic.")
    }
  }
}

case class ModCodeGen(
    left: GeneratedExpression,
    right: GeneratedExpression,
    resultType: InternalType)
  extends BinaryArithmeticCodeGen {

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    if (isNumeric(resultType)) {
      requireNumeric(left)
      requireNumeric(right)
      codegenBinaryArithmetic(ctx, "%")
    } else {
      throw new CodeGenException("Unsupported arithmetic.")
    }
  }
}

case class UnaryMinusCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {
  override def resultType: InternalType = operand.resultType

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    nullSafeCodeGen(ctx) {
      term =>
        if (isDecimal(operand.resultType)) {
          s"$term.negate()"
        } else {
          s"-($term)"
        }
    }
  }
}

case class UnaryPlusCodeGen(operand: GeneratedExpression) extends UnaryExprCodeGen {
  override def resultType: InternalType = operand.resultType

  override def codegen(ctx: CodeGeneratorContext): GeneratedExpression = {
    nullSafeCodeGen(ctx) {
      term =>
        if (isDecimal(operand.resultType)) {
          term
        } else {
          s"+($term)"
        }
    }
  }
}






