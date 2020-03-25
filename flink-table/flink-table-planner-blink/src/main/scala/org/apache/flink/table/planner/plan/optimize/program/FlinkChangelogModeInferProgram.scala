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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle}
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`.{ChangelogMode, ChangelogModeTrait, ChangelogModeTraitDef}
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDropUpdateBefore, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.planner.plan.utils.{ChangelogModeUtils, FlinkRelOptUtil}

import scala.collection.JavaConversions._

class FlinkChangelogModeInferProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(root: RelNode, context: StreamOptimizeContext): RelNode = {
    val initializedRoot = root.accept(new InitializeChangelogModeShuttle)
    val finalizedRoot = finalizeRootNodeChangelogMode(initializedRoot, context)
    finalizedRoot.accept(new FinalizeChangelogModeShuttle)
  }

  private def finalizeRootNodeChangelogMode(root: RelNode, context: StreamOptimizeContext): RelNode = {
    val rootMode = getChangelogModeTraitValue(root)
    if (rootMode.isDefined && !context.requestBeforeImageOfUpdate) {
      val mode = rootMode.get
      val expectedMode = ChangelogModeUtils.removeBeforeImageForUpdates(mode)
      if (!mode.equals(expectedMode)) {
        val newTraitSet = root.getTraitSet.plus(new ChangelogModeTrait(Some(expectedMode)))
        return root.copy(newTraitSet, root.getInputs)
      }
    }
    root
  }

  private class InitializeChangelogModeShuttle extends BottomUpRelShuttle {
    override def transform(rel: StreamPhysicalRel): RelNode = {
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
        rel.copy(traitsWithChangelogMode, rel.getInputs)
      } else {
        rel
      }
    }

    private def initializeChangelogMode(node: StreamPhysicalRel): Option[ChangelogMode] = {
      node match {
        case _: Sink => None
        case _ =>
          // the trait value has been set, because it is shuttled from leaves up
          val inputModes = node.getInputs.map(getChangelogModeTraitValue(_).get)
          Some(node.producedChangelogMode(inputModes.toArray))
      }
    }
  }

  private class FinalizeChangelogModeShuttle extends TopDownRelShuttle {
    override def transform(current: StreamPhysicalRel): RelNode = {
      val expectedOutputMode = getChangelogModeTraitValue(current).orNull
      val newInputs = current.getInputs.zipWithIndex.map { case (input, ordinal) =>
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
            case _ =>
              input.copy(newTraitSet, input.getInputs)
          }
        } else {
          input
        }
      }

      // update current if any input was updated
      if (current.getInputs != newInputs) {
        current.copy(current.getTraitSet, newInputs)
      } else {
        current
      }
    }
  }

  // -------------------------------------------------------------------------------------------

  private def getChangelogModeTraitValue(node: RelNode): Option[ChangelogMode] = {
    val changelogModeTrait = node.getTraitSet.getTrait(ChangelogModeTraitDef.INSTANCE)
    changelogModeTrait.changelogMode
  }

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



  // -------------------------------------------------------------------------------------------
  /**
   * Match from leaves up. A match attempt at a descendant precedes all match
   * attempts at its ancestors.
   */
  private abstract class BottomUpRelShuttle extends AbstractRelShuttle {
    override def visit(rel: RelNode): RelNode = {
      val newInputs = rel.getInputs.map(input => input.accept(this))
      val newRel = if (rel.getInputs != newInputs) {
        rel.copy(rel.getTraitSet, newInputs)
      } else {
        rel
      }
      transform(newRel.asInstanceOf[StreamPhysicalRel])
    }
  }

  /**
   * Match from root down. A match attempt at an ancestor always precedes all
   * match attempts at its descendants.
   */
  private abstract class TopDownRelShuttle extends AbstractRelShuttle {
    override def visit(rel: RelNode): RelNode = {
      val transformed = transform(rel.asInstanceOf[StreamPhysicalRel])
      val newInputs = transformed.getInputs.map(input => input.accept(this))
      if (rel.getInputs != newInputs) {
        transformed.copy(transformed.getTraitSet, newInputs)
      } else {
        transformed
      }
    }
  }

  private abstract class AbstractRelShuttle extends RelShuttle {

    def transform(rel: StreamPhysicalRel): RelNode

    override def visit(intersect: LogicalIntersect): RelNode = visit(intersect.asInstanceOf[RelNode])

    override def visit(union: LogicalUnion): RelNode = visit(union.asInstanceOf[RelNode])

    override def visit(aggregate: LogicalAggregate): RelNode = visit(aggregate.asInstanceOf[RelNode])

    override def visit(minus: LogicalMinus): RelNode = visit(minus.asInstanceOf[RelNode])

    override def visit(sort: LogicalSort): RelNode = visit(sort.asInstanceOf[RelNode])

    override def visit(`match`: LogicalMatch): RelNode = visit(`match`.asInstanceOf[RelNode])

    override def visit(exchange: LogicalExchange): RelNode = visit(exchange.asInstanceOf[RelNode])

    override def visit(scan: TableScan): RelNode = visit(scan.asInstanceOf[RelNode])

    override def visit(scan: TableFunctionScan): RelNode = visit(scan.asInstanceOf[RelNode])

    override def visit(values: LogicalValues): RelNode = visit(values.asInstanceOf[RelNode])

    override def visit(filter: LogicalFilter): RelNode = visit(filter.asInstanceOf[RelNode])

    override def visit(project: LogicalProject): RelNode = visit(project.asInstanceOf[RelNode])

    override def visit(join: LogicalJoin): RelNode = visit(join.asInstanceOf[RelNode])

    override def visit(correlate: LogicalCorrelate): RelNode = visit(correlate.asInstanceOf[RelNode])
  }

}
