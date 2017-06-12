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
package org.apache.flink.table.plan.nodes.datastream

import java.util
import java.math.{BigDecimal => JBigDecimal}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex._
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.cep.{CEP, PatternStream}
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableConfig, TableException}
import org.apache.flink.table.codegen.MatchCodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.cep.{ConvertToRow, IterativeConditionRunner, PatternSelectFunctionRunner}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.types.Row


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
    
    val crowInput: DataStream[CRow] = getInput
      .asInstanceOf[DataStreamRel]
      .translateToPlan(tableEnv, queryConfig)

    val inputDS: DataStream[Row] = crowInput
      .map(new ConvertToRow)
      .setParallelism(crowInput.getParallelism)
      .name("ConvertToRow")
      .returns(inputTypeInfo)

    def translatePattern(rexNode: RexNode, pattern: Pattern[Row, Row]): Pattern[Row, Row] = rexNode match {
      case literal: RexLiteral =>
        val name = parseToString(literal)
        val previousPatternName = if (pattern == null) null else pattern.getName
        val newPattern = next(pattern, name)

        val definition = patternDefinitions.get(name)
        if (definition != null) {
          val condition = generateCondition(
            name,
            previousPatternName,
            definition,
            config,
            inputTypeInfo)

          newPattern.where(condition)
        } else {
          newPattern
        }

      case call: RexCall =>

        call.getOperator match {
          case PATTERN_CONCAT =>
            val left = call.operands.get(0)
            val right = call.operands.get(1)
            translatePattern(right, translatePattern(left, pattern))

          case PATTERN_QUANTIFIER =>
            val name = call.operands.get(0).asInstanceOf[RexLiteral]
            val newPattern = translatePattern(name, pattern)

            val startNum = parseToInt(call.operands.get(1).asInstanceOf[RexLiteral])
            val endNum = parseToInt(call.operands.get(2).asInstanceOf[RexLiteral])

            if (startNum == 0 && endNum == -1) {  // zero or more
              throw TableException("Currently, CEP doesn't support zeroOrMore (kleene star) operator.")
            } else if (startNum == 1 && endNum == -1) { // one or more
              newPattern.oneOrMore()
            } else if (startNum == endNum) {   // times
              newPattern.times(startNum)
            } else if (startNum == 0 && endNum == 1) {  // optional
              newPattern.optional()
            } else {
              throw TableException(s"Currently, CEP doesn't support '{$startNum, $endNum}' quantifier.")
            }

          case PATTERN_ALTER =>
            throw TableException("Currently, CEP doesn't support branching patterns.")

          case PATTERN_PERMUTE =>
            throw TableException("Currently, CEP doesn't support PERMUTE patterns.")

          case PATTERN_EXCLUDE =>
            throw TableException("Currently, CEP doesn't support '{-' '-}' patterns.")
        }

      case _ =>
        throw TableException("")
    }

    val cepPattern = translatePattern(pattern, null)
    val patternStream: PatternStream[Row] = CEP
      .pattern[Row](inputDS, cepPattern)

    val internalType = CRowTypeInfo(inputTypeInfo)
    // FIXME: currently, MEASURES not supported, so hardcode output for test purpose
    patternStream.select[CRow](new PatternSelectFunctionRunner(cepPattern.getName), internalType)
  }


  private def generateCondition(
      patternName: String,
      previousPatternName: String,
      definition: RexNode,
      config: TableConfig,
      inputTypeInfo: TypeInformation[_]): IterativeCondition[Row] = {

    val generator = new MatchCodeGenerator(config, inputTypeInfo, patternName, previousPatternName)
    val condition = generator.generateExpression(inputSchema.mapRexNode(definition))
    val body =
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
       """.stripMargin

    val genCondition = generator.generateIterativeCondition("MatchRecognizeCondition", body)
    new IterativeConditionRunner(genCondition.name, genCondition.code)
  }


  private def next(pattern: Pattern[Row, Row], name: String): Pattern[Row, Row] = {
    if (pattern == null) {
      Pattern.begin(name)
    } else {
      pattern.next(name)
    }
  }

  private def patternName(pattern: Pattern[Row, Row]): String = {
    if (pattern == null) {
      null
    } else {
      pattern.getPrevious.getName
    }
  }

  private def parseToInt(literal: RexLiteral): Int = {
    literal.getValue3.asInstanceOf[JBigDecimal].intValue()
  }

  private def parseToString(literal: RexLiteral): String = {
    literal.getValue3.toString
  }
}
