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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDropUpdateBefore, StreamExecExchange, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.planner.plan.utils.{ChangelogModeUtils, FlinkRelOptUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}

import java.util.Collections

import scala.collection.JavaConversions._

object InferChangelogModeRules {

  val PROPAGATE_CHANGELOG_MODE_INSTANCE = new PropagateChangelogModeRule
  val FINALIZE_CHANGELOG_MODE_INSTANCE = new FinalizeChangelogModeRule
  val EXCHANGE_DROP_UPDATE_BEFORE_TRANSPOSE_INSTANCE = new ExchangeDropUpdateBeforeTransposeRule
  val CHANGELOG_ORDER_PRESERVING_INSTANCE = new ChangelogOrderPreservingRule

  class PropagateChangelogModeRule
    extends RelOptRule(
      operand(classOf[StreamPhysicalRel], none()),
      "PropagateChangelogModeRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val rel: StreamPhysicalRel = call.rel(0)
      val inputs = getInputRelNodes(rel)
      inputs.isEmpty || inputs.forall(getChangelogModeTraitValue(_).isDefined)
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel: StreamPhysicalRel = call.rel(0)
      val traits = rel.getTraitSet

      val changelogModeTrait = traits.getTrait(ChangelogModeTraitDef.INSTANCE)
      val initialChangelogMode = initializeChangelogMode(rel)
      val traitsWithChangelogMode =
        if (changelogModeTrait.changelogMode != initialChangelogMode) {
          traits.plus(new ChangelogModeTrait(initialChangelogMode))
        } else {
          traits
        }

      if (traits != traitsWithChangelogMode) {
        val newRel = rel.copy(traitsWithChangelogMode, rel.getInputs)
        call.transformTo(newRel)
      }
    }

    private def initializeChangelogMode(node: StreamPhysicalRel): Option[ChangelogMode] = {
      node match {
        case _: Sink => None
        case _ =>
          // the trait value has been set, because we verified it in matches()
          val inputModes = getInputRelNodes(node).map(getChangelogModeTraitValue(_).get)
          Some(node.producedChangelogMode(inputModes.toArray))
      }
    }
  }

  class FinalizeChangelogModeRule
    extends RelOptRule(
      operand(classOf[StreamPhysicalRel], none()),
      "FinalizeChangelogModeRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {
      val current: StreamPhysicalRel = call.rel(0)
      val expectedOutputMode = getChangelogModeTraitValue(current).orNull
      val inputs = getInputRelNodes(current)
      val newInputs = inputs.zipWithIndex.map { case (input, ordinal) =>
        val inputMode = getChangelogModeTraitValue(input).get
        val expectedInputMode = current.consumedChangelogMode(
          ordinal,
          inputMode,
          expectedOutputMode)
        if (!inputMode.equals(expectedInputMode)) {
          validateSatisfaction(inputMode, expectedInputMode, current)
          val traitSet = input.getTraitSet
          val newTraitSet = traitSet.plus(new ChangelogModeTrait(Some(expectedInputMode)))
          input match {
            case t: StreamExecIntermediateTableScan if t.intermediateTable.isUpdateBeforeRequired =>
              new StreamExecDropUpdateBefore(input.getCluster, newTraitSet, input)
//            case ex: StreamExecExchange if getInputRelNodes(ex).isInstanceOf[]
            case _ =>
              input.copy(newTraitSet, input.getInputs)
          }
        } else {
          input
        }
      }

      // update current if any input was updated
      if (inputs != newInputs ) {
        val newRel = current.copy(current.getTraitSet, newInputs)
        call.transformTo(newRel)
      }
    }
  }

  class ExchangeDropUpdateBeforeTransposeRule extends RelOptRule(
    operand(classOf[StreamExecExchange],
      operand(classOf[StreamExecDropUpdateBefore], any())),
    "ExchangeDropUpdateBeforeTransposeRule") {

    override def onMatch(call: RelOptRuleCall): Unit = {
      val exchange = call.rel[StreamExecExchange](0)
      val dropBefore = call.rel[StreamExecDropUpdateBefore](1)
      val inputChangelogMode = getChangelogModeTraitValue(dropBefore.getInput)
      val newTraitSet = exchange.getTraitSet.plus(new ChangelogModeTrait(inputChangelogMode))
      val newExchange = exchange.copy(newTraitSet, dropBefore.getInputs)
      val newDropBefore = dropBefore.copy(
        dropBefore.getTraitSet,
        Collections.singletonList(newExchange))
      call.transformTo(newDropBefore)
    }
  }

  class ChangelogOrderPreservingRule extends RelOptRule(
    operand(classOf[StreamPhysicalRel],
      operand(classOf[StreamExecIntermediateTableScan], any())),
    "ChangelogOrderPreservingRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val rel = call.rel[StreamPhysicalRel](0)
      val intermediate = call.rel[StreamExecIntermediateTableScan](1)
      val changelogMode = getChangelogModeTraitValue(intermediate).get
      val containsUpdateOrDelete = changelogMode.contains(RowKind.UPDATE_AFTER) ||
        changelogMode.contains(RowKind.DELETE)
      containsUpdateOrDelete && !rel.isInstanceOf[StreamExecExchange]
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val rel = call.rel[StreamPhysicalRel](0)
      val intermediate = call.rel[StreamExecIntermediateTableScan](1)
      val uniqueKeys = call.getMetadataQuery.getUniqueKeys(intermediate.intermediateTable.relNode)
      val requiredDistribution = if (!uniqueKeys.isEmpty) {
        FlinkRelDistribution.hash(uniqueKeys.head.asList())
      } else {
        FlinkRelDistribution.SINGLETON
      }
      val requiredTraitSet = intermediate.getTraitSet.plus(requiredDistribution)
      val exchange = new StreamExecExchange(
        intermediate.getCluster,
        requiredTraitSet,
        intermediate,
        requiredDistribution)
      val newRel = rel.copy(rel.getTraitSet, Collections.singletonList(exchange))
      call.transformTo(newRel)
    }
  }

  // ------------------------------------------------------------------------------------------
  // Utilities
  // ------------------------------------------------------------------------------------------

  private def validateSatisfaction(
      oldMode: ChangelogMode,
      newMode: ChangelogMode,
      root: RelNode): Unit = {
    val builder = ChangelogMode.newBuilder()
    oldMode.getContainedKinds.foreach { kind =>
      if (kind != RowKind.UPDATE_BEFORE) {
        builder.addContainedKind(kind)
      }
    }
    val oldModeWithoutBefore = builder.build()
    if (!oldMode.equals(newMode) && !oldModeWithoutBefore.equals(newMode)) {
      val previous = ChangelogModeUtils.stringifyChangelogMode(oldMode)
      val current = ChangelogModeUtils.stringifyChangelogMode(newMode)
      val plan = FlinkRelOptUtil.toString(root, withRetractTraits = true)
      throw new IllegalStateException(
        s"Previous changelog mode [$previous] can't be changed to [$current], this is a bug. " +
        s"Plan:\n" + plan)
    }
  }

  /**
   * Gets all input RelNodes of a RelNode.
   */
  private def getInputRelNodes(node: RelNode): Seq[RelNode] = {
    node.getInputs.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  private def getChangelogModeTraitValue(node: RelNode): Option[ChangelogMode] = {
    val changelogModeTrait = node.getTraitSet.getTrait(ChangelogModeTraitDef.INSTANCE)
    changelogModeTrait.changelogMode
  }

  private def getSendBeforeImageTraitValue(node: RelNode): Boolean = {
    val emitUpdateBeforeTrait = node.getTraitSet
      .getTrait(SendBeforeImageForUpdatesTraitDef.INSTANCE)
    emitUpdateBeforeTrait.sendBeforeImageForUpdates
  }

}
