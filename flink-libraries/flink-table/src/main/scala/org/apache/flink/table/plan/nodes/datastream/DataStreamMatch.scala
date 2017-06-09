package org.apache.flink.table.plan.nodes.datastream

import java.util
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.`type`.SqlTypeName.{CHAR, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow


/**
  * Flink DataStream RelNode for LogicalMatch
  */
class DataStreamMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    pattern: RexNode,
    strictStart: Boolean,
    strictEnd: Boolean,
    patternDefinitions: util.Map[String, RexNode],
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamMatch(
      cluster,
      traitSet,
      inputs.get(0),
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      schema,
      inputSchema)
  }

  override def toString: String = {
    "Match"
  }


  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val config = tableEnv.config
    val inputTypeInfo = inputSchema.physicalTypeInfo
    val inputDS = getInput.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    // TODO

    def translatePattern(rexNode: RexNode, pattern: Pattern[_, _]): Pattern[_, _] = rexNode match {
      case literal: RexLiteral =>
        val name = parseToString(literal)
        next(pattern, name)

      case call: RexCall =>

        call.getOperator match {
          case PATTERN_CONCAT =>
            val left = call.operands.get(0)
            val right = call.operands.get(1)
            translatePattern(right, translatePattern(left, pattern))

          case PATTERN_QUANTIFIER =>
            val name = parseToString(call.operands.get(0).asInstanceOf[RexLiteral])
            val startNum = parseToInt(call.operands.get(1).asInstanceOf[RexLiteral])
            val endNum = parseToInt(call.operands.get(2).asInstanceOf[RexLiteral])
            val newPattern = next(pattern, name) //.where

            val definition = patternDefinitions.get(name)
            if (definition != null) {
              val condition = inputSchema.mapRexNode(definition)
              val generator = new CodeGenerator(config, false, inputTypeInfo)
              val body = generator.generateExpression(condition)

            }



            if (startNum == 0 && endNum == -1) {  // zero or more
              throw TableException("Currently, CEP doesn't support zeroOrMore (kleene star) operator.")
            } else if (startNum == 1 && endNum == -1) { // one or more
              newPattern.oneOrMore()
            } else if (startNum == endNum) {   // times
              newPattern.times(startNum)
            } else {
              throw TableException(s"Currently, CEP doesn't support '{$startNum, $endNum}' quantifier.")
            }

          case PATTERN_ALTER =>
            throw TableException("Currently, CEP doesn't support branching patterns.")

          case PATTERN_PERMUTE | PATTERN_EXCLUDE =>
            throw TableException("Currently, CEP doesn't support PERMUTE and '{-' '-}' patterns.")

        }

      case _ =>
        throw TableException("")


    }


    null
  }



  private def next(pattern: Pattern[_, _], name: String): Pattern[_, _] = {
    if (pattern == null) {
      Pattern.begin(name)
    } else {
      pattern.next(name)
    }
  }

  private def parseToInt(literal: RexLiteral): Int = {
    literal.getValue3.asInstanceOf[JBigDecimal].intValue()
  }

  private def parseToString(literal: RexLiteral): String = {
    literal.getValue3.toString
  }

  private def parseToBoolean(literal: RexLiteral): Boolean = {
    literal.getValue3.toString.toBoolean
  }



}
