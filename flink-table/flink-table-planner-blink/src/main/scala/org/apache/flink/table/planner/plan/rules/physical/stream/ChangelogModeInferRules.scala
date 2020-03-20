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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{none, operand}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`.{ChangelogMode, ChangelogModeTrait, ChangelogModeTraitDef, SendBeforeImageForUpdatesTrait, SendBeforeImageForUpdatesTraitDef}
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecGroupAggregate, StreamExecOverAggregate, StreamPhysicalRel}

import scala.collection.JavaConversions._

object ChangelogModeInferRules {

  val PROPAGATE_CHANGELOG_MODE_INSTANCE = new PropagateChangelogModeRule

  val PROPAGATE_EMIT_UPDATE_BEFORE_INSTANCE = new PropagateSendBeforeImageRule

  val FINALIZE_CHANGELOG_MODE_INSTANCE = new FinalizeChangelogModeRule

  // ------------------------------------------------------------------------------------------

  /**
   * Rule that assigns an initial [[ChangelogModeTrait]] to all [[StreamPhysicalRel]] nodes.
   * The propagate direction is from upstream to downstream.
   */
  class PropagateChangelogModeRule
    extends RelOptRule(
      operand(classOf[StreamPhysicalRel], none()),
      "PropagateChangelogModeRule") {

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
          val builder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
          if (node.produceUpdates) {
            // we don't add UPDATE BEFORE to the initial ChangelogMode
            // which will be decided at the last stage, i.e. FinalizeChangelogModeRule
            builder.addContainedKind(RowKind.UPDATE_AFTER)
          }
          if (node.produceDeletions) {
            builder.addContainedKind(RowKind.DELETE)
          }
          if (node.forwardChanges) {
            getInputRelNodes(node).foreach { input =>
              val inputChangelogMode = getChangelogModeTraitValue(input)
              inputChangelogMode.getContainedKinds.foreach(builder.addContainedKind)
            }
          }
          Some(builder.build())
      }
    }
  }

  // ------------------------------------------------------------------------------------------

  /**
   * Rule that marks all [[StreamPhysicalRel]] nodes that emit update before message or not.
   * The propagate direction is from downstream to upstream.
   */
  class PropagateSendBeforeImageRule
    extends RelOptRule(
      operand(classOf[StreamPhysicalRel], none()),
      "PropagateEmitUpdateBeforeRule") {

    /**
     * A [[RelNode]] needs to emit update before messages, if
     *
     * 1. its downstream request update before messages by itself because it is a certain type
     * of operator, such as a [[StreamExecGroupAggregate]] or [[StreamExecOverAggregate]], or
     * 2. its downstream request update before message because its own parent request it
     * (transitive request).
     */
    override def onMatch(call: RelOptRuleCall): Unit = {
      val current: StreamPhysicalRel = call.rel(0)
      val transitiveRequest = getSendBeforeImageTraitValue(current) && current.forwardChanges
      val inputs = getInputRelNodes(current)
      val newInputs = inputs.map { input =>
        val request = current.requestBeforeImageOfUpdates(input)
        if (transitiveRequest || request) {
          // set EmitUpdateBefore to true
          val traitSet = input.getTraitSet
          val newTraitSet = traitSet.plus(new SendBeforeImageForUpdatesTrait(true))
          input.copy(newTraitSet, input.getInputs)
        } else {
          input
        }
      }

      // update current if any input was updated
      if (inputs != newInputs) {
        val newRel = current.copy(current.getTraitSet, newInputs)
        call.transformTo(newRel)
      }
    }
  }

  // ------------------------------------------------------------------------------------------

  /**
   * Sets the [[ChangelogMode]] of [[StreamPhysicalRel]] nodes.
   */
  class FinalizeChangelogModeRule
    extends RelOptRule(
      operand(classOf[StreamPhysicalRel], none()),
      "FinalizeChangelogModeRule") {

    override def matches(call: RelOptRuleCall): Boolean = {
      val current: StreamPhysicalRel = call.rel(0)
      current match {
        case _: Sink => false
        case _ =>
          val changelogMode = getChangelogModeTraitValue(current)
          val containsUpdateBefore = changelogMode.contains(RowKind.UPDATE_BEFORE)
          // only set UPDATE_BEFORE if current node produce update_before
          // or it forwards update_before from inputs
          !containsUpdateBefore && (produceUpdateBefore(current) || forwardUpdateBefore(current))
      }
    }

    override def onMatch(call: RelOptRuleCall): Unit = {
      val current: StreamPhysicalRel = call.rel(0)
      val changelogMode = getChangelogModeTraitValue(current)
      val builder = ChangelogMode.newBuilder()
      builder.addContainedKind(RowKind.UPDATE_BEFORE)
      changelogMode.getContainedKinds.foreach(builder.addContainedKind)
      val newChangelogMode = Some(builder.build())

      // update current RelNode
      val traitSet = current.getTraitSet
      val newTraitSet = traitSet.plus(new ChangelogModeTrait(newChangelogMode))
      val newRel = current.copy(newTraitSet, current.getInputs)
      call.transformTo(newRel)
    }

    private def produceUpdateBefore(current: StreamPhysicalRel): Boolean = {
      val emitUpdateBefore = getSendBeforeImageTraitValue(current)
      emitUpdateBefore && current.produceUpdates
    }

    private def forwardUpdateBefore(current: StreamPhysicalRel): Boolean = {
      if (!current.forwardChanges) {
        return false
      }
      val containsUpdateChanges = getInputRelNodes(current).exists { input =>
        val changelogMode = getChangelogModeTraitValue(input)
        changelogMode.contains(RowKind.UPDATE_AFTER)
      }
      containsUpdateChanges
    }
  }

  // ------------------------------------------------------------------------------------------
  // Utilities
  // ------------------------------------------------------------------------------------------

  /**
   * Gets all input RelNodes of a RelNode.
   */
  private def getInputRelNodes(node: RelNode): Seq[RelNode] = {
    node.getInputs.map(_.asInstanceOf[HepRelVertex].getCurrentRel)
  }

  private def getSendBeforeImageTraitValue(node: RelNode): Boolean = {
    val emitUpdateBeforeTrait = node.getTraitSet
      .getTrait(SendBeforeImageForUpdatesTraitDef.INSTANCE)
    emitUpdateBeforeTrait.sendBeforeImageForUpdates
  }

  private def getChangelogModeTraitValue(node: RelNode): ChangelogMode = {
    val changelogModeTrait = node.getTraitSet.getTrait(ChangelogModeTraitDef.INSTANCE)
    val mode = changelogModeTrait.changelogMode
    if (mode.isEmpty) {
      throw new IllegalAccessException(
        s"${node.getClass.getSimpleName} doesn't have a ChangelogMode.")
    }
    mode.get
  }

}
