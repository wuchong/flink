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
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`.{ChangelogMode, ChangelogModeTrait, ChangelogModeTraitDef}
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.utils.ChangelogModeUtils._
import org.apache.flink.table.planner.plan.utils.{ChangelogModeUtils, FlinkRelOptUtil, RankProcessStrategy, RetractStrategy}
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, UpsertStreamTableSink}

import scala.collection.JavaConversions._

class FlinkChangelogModeInferProgram3 extends FlinkOptimizeProgram[StreamOptimizeContext] {

  private val INITIALIZE_CHANGELOG_MODE = new InitializeChangelogModeVisitor
  private val FINALIZE_CHANGELOG_MODE = new FinalizeChangelogModeVisitor

  override def optimize(
      root: RelNode,
      context: StreamOptimizeContext): RelNode = {
    val physicalRoot = root.asInstanceOf[StreamPhysicalRel]
    // TODO: get allowedMode from context
    val initializedRoot = INITIALIZE_CHANGELOG_MODE.matchAllowedChangelogMode(physicalRoot, ALL_CHANGES, "TODO")
    val requiredChangelogMode = getRequiredChangelogMode(initializedRoot, context)
    val finalizedRoot = FINALIZE_CHANGELOG_MODE.satisfyRequiredChangelogMode(initializedRoot, requiredChangelogMode)
    finalizedRoot
  }

  private def getRequiredChangelogMode(root: RelNode, context: StreamOptimizeContext): ChangelogMode = {
    // TODO: get requiredMode from context
    val changelogMode = getChangelogModeTraitValue(root)
    if (changelogMode != null) {
      removeBeforeImageForUpdates(changelogMode)
    } else {
      changelogMode
    }
  }


  private class InitializeChangelogModeVisitor {

    /**
     * Try to match the allowed ChangelogMode for current node which is requested by requestOwner.
     *
     * <p>The allowing request is triggered from root, then
     * 1) every node will request an allowedMode for its children, the allowedMode maybe from
     *    it's parent or from itself. This will request from root down, and match from leaves up.
     * 2) validate whether the produced changelog mode of children matches the allowed mode.
     * 3) convert current node into a new node with new children and produced changelog mode of
     *    current node.
     *
     * @param rel current node
     * @param allowedMode allowed ChangelogMode
     * @param requestOwner the owner who starts the allowedMode request, this is used for
     *                     a better exception message to tell users the plan is not allowed by who
     * @return A converted node which matches the allowed ChangelogMode.
     */
    def matchAllowedChangelogMode(
        rel: StreamPhysicalRel,
        allowedMode: ChangelogMode,
        requestOwner: String): StreamPhysicalRel = {
      rel match {
        case sink: StreamExecSink[_] =>
          // ignore the context request mode, because sink is the true root
          val (supported, name) = sink.sink match {
            case _: AppendStreamTableSink[_] =>
              (ChangelogModeUtils.INSERT_ONLY, "AppendStreamTableSink")
            case _: UpsertStreamTableSink[_] =>
              (ChangelogModeUtils.ALL_CHANGES, "UpsertStreamTableSink")
            case  _: RetractStreamTableSink[_] =>
              (ChangelogModeUtils.ALL_CHANGES, "RetractStreamTableSink")
            case _ =>
              throw new UnsupportedOperationException(
                s"Unsupported sink '${sink.sink.getClass.getSimpleName}'")
          }
          val children = visitChildren(sink, supported, name)
          // we do not update sink ChangelogMode trait, because it should be none.
          sink.copy(sink.getTraitSet, children).asInstanceOf[StreamPhysicalRel]

        case deduplicate: StreamExecDeduplicate =>
          // deduplicate only support insert only as input
          val children = visitChildren(deduplicate, ChangelogModeUtils.INSERT_ONLY)
          val builder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
          if (deduplicate.keepLastRow) {
            // produce updates
            builder.addContainedKind(RowKind.UPDATE_BEFORE).addContainedKind(RowKind.UPDATE_AFTER)
          }
          // will not produce deletion
          val producedMode = builder.build()
          replaceInputsAndChangelogMode(deduplicate, children, producedMode)

        case agg: StreamExecGroupAggregate =>
          // agg support all changes in input
          val children = visitChildren(agg, ChangelogModeUtils.ALL_CHANGES)
          val inputMode = getChangelogModeTraitValue(children.head)
          val builder = ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_BEFORE)
            .addContainedKind(RowKind.UPDATE_AFTER)
          if (inputMode.contains(RowKind.UPDATE_AFTER) || inputMode.contains(RowKind.DELETE)) {
            builder.addContainedKind(RowKind.DELETE)
          }
          val producedMode = builder.build()
          replaceInputsAndChangelogMode(agg, children, producedMode)

        case tagg: StreamExecGroupTableAggregate =>
          // table aggregate support all changes in input
          val children = visitChildren(tagg, ChangelogModeUtils.ALL_CHANGES)
          // table aggregate will produce all changes, including deletions
          replaceInputsAndChangelogMode(tagg, children, ChangelogModeUtils.ALL_CHANGES)

        case window: StreamExecGroupWindowAggregateBase =>
          // window aggregate support insert only in input
          val children = visitChildren(window, ChangelogModeUtils.INSERT_ONLY)
          val builder = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT)
          if (window.emitStrategy.produceUpdates) {
            builder.addContainedKind(RowKind.UPDATE_BEFORE).addContainedKind(RowKind.UPDATE_AFTER)
          }
          val producedMode = builder.build()
          replaceInputsAndChangelogMode(rel, children, producedMode)

        case limit: StreamExecLimit =>
          // limit support all changes in input
          val children = visitChildren(rel, ChangelogModeUtils.ALL_CHANGES)
          val producedMode = if (isInsertOnly(getChangelogModeTraitValue(children.head))) {
            ChangelogModeUtils.INSERT_ONLY
          } else {
            ChangelogModeUtils.ALL_CHANGES
          }
          replaceInputsAndChangelogMode(limit, children, producedMode)

        case rank: StreamExecRank =>
          // Rank supports consuming all changes
          val children = visitChildren(rel, ChangelogModeUtils.ALL_CHANGES)
          val builder = ChangelogMode.newBuilder()
            .addContainedKind(RowKind.INSERT)
            .addContainedKind(RowKind.UPDATE_BEFORE)
            .addContainedKind(RowKind.UPDATE_AFTER)
          if (!rank.outputRankNumber || rank.getStrategy(forceRecompute = true) == RetractStrategy) {
            // 1) produce deletion if without row_number,
            //    because we use deletion to remove records out of TopN
            // 2) AppendStrategy or UpdateStrategy will not produce deletions
            builder.addContainedKind(RowKind.DELETE)
          }
          val producedMode = builder.build()
          replaceInputsAndChangelogMode(rank, children, producedMode)

        case sortLimit: StreamExecSortLimit =>
          val children = visitChildren(sortLimit, ChangelogModeUtils.ALL_CHANGES)
          // SortLimit is the same to Rank without rank number in physical
          replaceInputsAndChangelogMode(sortLimit, children, ChangelogModeUtils.ALL_CHANGES)

        case sort: StreamExecSort =>
          // Sort supports consuming all changes
          val children = visitChildren(sort, ChangelogModeUtils.ALL_CHANGES)
          // Sort will buffer all inputs, and produce insert-only messages when input is finished
          replaceInputsAndChangelogMode(sort, children, ChangelogModeUtils.INSERT_ONLY)

        case _: StreamExecTemporalSort | _: StreamExecMatch |
             _: StreamExecOverAggregate | _: StreamExecWindowJoin =>
          // TemporalSort, CEP, OverAggregate, WindowJoin only support consuming insert-only
          // and producing insert-only changes
          val children = visitChildren(rel, ChangelogModeUtils.INSERT_ONLY)
          replaceInputsAndChangelogMode(rel, children, ChangelogModeUtils.INSERT_ONLY)

        case join: StreamExecJoin =>
          // join support all changes in input
          val children = visitChildren(rel, ChangelogModeUtils.ALL_CHANGES)
          val leftMode = getChangelogModeTraitValue(children.head)
          val rightMode = getChangelogModeTraitValue(children.last)
          val innerOrSemi = join.flinkJoinType == FlinkJoinType.INNER || join.flinkJoinType == FlinkJoinType.SEMI
          val producedMode = if (isInsertOnly(leftMode) && isInsertOnly(rightMode) && innerOrSemi) {
            ChangelogModeUtils.INSERT_ONLY
          } else {
            // only produce insert and delete
            ChangelogMode.newBuilder()
              .addContainedKind(RowKind.INSERT)
              .addContainedKind(RowKind.DELETE)
              .build()
          }
          replaceInputsAndChangelogMode(join, children, producedMode)

        case temporalJoin: StreamExecTemporalJoin =>
          val left = visitChild(temporalJoin, 0, ChangelogModeUtils.INSERT_ONLY, "TemporalJoin")
          val right = visitChild(temporalJoin, 1, ChangelogModeUtils.ALL_CHANGES, "TemporalJoin")
          // forward left input changes
          val producedMode = getChangelogModeTraitValue(left)
          replaceInputsAndChangelogMode(temporalJoin, List(left, right), producedMode)

        case _: StreamExecCalc | _: StreamExecPythonCalc | _: StreamExecCorrelate |
             _: StreamExecPythonCorrelate | _: StreamExecLookupJoin | _: StreamExecExchange |
             _: StreamExecExpand | _: StreamExecMiniBatchAssigner |
             _: StreamExecWatermarkAssigner =>
          // transparent forward allowedMode to children
          val children = visitChildren(rel, allowedMode, requestOwner)
          val inputMode = getChangelogModeTraitValue(children.head)
          // forward input mode
          replaceInputsAndChangelogMode(rel, children, inputMode)

        case union: StreamExecUnion =>
          // transparent forward allowedMode to children
          val children = visitChildren(rel, allowedMode, requestOwner)
          val producedMode = ChangelogModeUtils.union(children.map(getChangelogModeTraitValue): _*)
          replaceInputsAndChangelogMode(union, children, producedMode)

        case _: StreamExecDataStreamScan | _: StreamExecTableSourceScan | _: StreamExecValues =>
          // DataStream, TableSource and Values only support producing insert-only messages
          replaceInputsAndChangelogMode(rel, List(), ChangelogModeUtils.INSERT_ONLY)

        case intermediate: StreamExecIntermediateTableScan =>
          val producedMode = intermediate.intermediateTable.changelogMode
          replaceInputsAndChangelogMode(intermediate, List(), producedMode)

        case _ =>
          throw new UnsupportedOperationException(
            s"Unsupported visit for ${rel.getClass.getSimpleName}")
      }
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        allowedChildrenMode: ChangelogMode): List[StreamPhysicalRel] = {
      visitChildren(parent, allowedChildrenMode, getNodeName(parent))
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        allowedChildrenMode: ChangelogMode,
        requestOwner: String): List[StreamPhysicalRel] = {
      val newChildren = for (i <- 0 until parent.getInputs.size()) yield {
        visitChild(parent, i, allowedChildrenMode, requestOwner)
      }
      newChildren.toList
    }

    private def visitChild(
        parent: StreamPhysicalRel,
        childOrdinal: Int,
        allowedChildMode: ChangelogMode,
        requestOwner: String): StreamPhysicalRel = {
      val child = parent.getInput(childOrdinal).asInstanceOf[StreamPhysicalRel]
      val newChild = this.matchAllowedChangelogMode(child, allowedChildMode, requestOwner)
      val changelogMode = getChangelogModeTraitValue(newChild)
      validateChangelogModeAllowed(changelogMode, allowedChildMode, requestOwner)
      newChild
    }

    private def getNodeName(rel: StreamPhysicalRel): String = {
      val prefix = "StreamExec"
      val typeName = rel.getRelTypeName
      if (typeName.startsWith(prefix)) {
        typeName.substring(prefix.length)
      } else {
        typeName
      }
    }

    private def validateChangelogModeAllowed(
      mode: ChangelogMode,
      requestedMode: ChangelogMode,
      requestedOwner: String): Unit = {
      if (mode.contains(RowKind.UPDATE_AFTER) && !requestedMode.contains(RowKind.UPDATE_AFTER)) {
        throw new TableException(
          s"$requestedOwner doesn't support consuming update changes. " +
            s"Please check whether $requestedOwner is followed after a regular aggregation " +
            s"or regular join which produces update changes.")
      }
      if (mode.contains(RowKind.DELETE) && !requestedMode.contains(RowKind.DELETE)) {
        throw new TableException(s"$requestedOwner doesn't support con  suming deletion changes. " +
          s"Please check whether $requestedOwner is followed after a nested regular aggregation " +
          s"or a left join which produces deletion changes.")
      }
      if (mode.contains(RowKind.INSERT) && !requestedMode.contains(RowKind.INSERT)) {
        throw new TableException(s"$requestedOwner doesn't support consuming insert messages. " +
          s"This should be a bug in implementation of $requestedOwner, " +
          "because all the operators should support consuming insert messages")
      }
      // we do not validate UPDATE_BEFORE, because it is special row kind which is optional
    }

  }

  private class FinalizeChangelogModeVisitor {

    /**
     * Try to satisfy the required ChangelogMode by children of current node.
     *
     * <p>If children can satisfy required ChangelogMode, and current node will not destroy it,
     * then returns the new node with converted inputs, e.g. Calc, Correlate.
     *
     * <p>If current node will destroy input ChangelogMode, then try to convert itself to satisfy
     * required ChangelogMode, e.g. Aggregate, Rank.
     *
     * @param rel current node
     * @param requiredMode required ChangelogMode
     * @return A converted node which satisfy required ChangelogMode.
     */
    def satisfyRequiredChangelogMode(
        rel: StreamPhysicalRel,
        requiredMode: ChangelogMode): StreamPhysicalRel = rel match {
      case sink: StreamExecSink[_] =>
        val inputMode = getChangelogModeTraitValue(sink.getInput)
        val requestedInputMode = sink.sink match {
          case _: UpsertStreamTableSink[_] =>
            ChangelogModeUtils.removeBeforeImageForUpdates(inputMode)
          case  _: AppendStreamTableSink[_] | _: RetractStreamTableSink[_] =>
            inputMode
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported sink '${sink.sink.getClass.getSimpleName}'")
        }
        val children = visitChildren(sink, requestedInputMode)
        // we do not update sink ChangelogMode trait, because it should be none.
        sink.copy(sink.getTraitSet, children).asInstanceOf[StreamPhysicalRel]

      case _: StreamExecGroupAggregate | _: StreamExecGroupTableAggregate |
           _: StreamExecLimit =>
        // Aggregate, TableAggregate and Limit requires update_before if there are updates
        val requiredChildMode = addBeforeImageForUpdates(getChangelogModeTraitValue(rel.getInput(0)))
        val children = visitChildren(rel, requiredChildMode)
        convertToRequiredChangelogMode(rel, children, requiredMode)

      case _: StreamExecGroupWindowAggregate | _: StreamExecGroupWindowTableAggregate |
           _: StreamExecDeduplicate | _: StreamExecTemporalSort | _: StreamExecMatch |
           _: StreamExecOverAggregate | _: StreamExecWindowJoin=>
        // WindowAggregate, WindowTableAggregate, Deduplicate, TemporalSort, CEP, OverAggregate
        // and WindowJoin require insert only in input
        val children = visitChildren(rel, ChangelogModeUtils.INSERT_ONLY)
        convertToRequiredChangelogMode(rel, children, requiredMode)

      case rank: StreamExecRank =>
        visitRankOrSortLimit(rank, rank.getStrategy(forceRecompute = true), requiredMode)

      case sortLimit: StreamExecSortLimit =>
        visitRankOrSortLimit(sortLimit, sortLimit.getStrategy(forceRecompute = true), requiredMode)

      case sort: StreamExecSort =>
        val childMode = getChangelogModeTraitValue(sort.getInput)
        val requiredChildMode = addBeforeImageForUpdates(childMode)
        val children = visitChildren(rel, requiredChildMode)
        convertToRequiredChangelogMode(rel, children, requiredMode)

      case join: StreamExecJoin =>
        val children = List(0, 1).map { inputOrdinal =>
          val leftNeedUpdateBefore = !join.inputUniqueKeyContainsJoinKey(inputOrdinal)
          val inputMode = getChangelogModeTraitValue(join.getInput(inputOrdinal))
          val leftRequiredMode = if (leftNeedUpdateBefore) {
            addBeforeImageForUpdates(inputMode)
          } else {
            removeBeforeImageForUpdates(inputMode)
          }
          this.satisfyRequiredChangelogMode(join.getInput(inputOrdinal).asInstanceOf[StreamPhysicalRel], leftRequiredMode)
        }
        convertToRequiredChangelogMode(rel, children, requiredMode)

      case temporalJoin: StreamExecTemporalJoin =>
        // forward required mode to left input
        val left = temporalJoin.getLeft.asInstanceOf[StreamPhysicalRel]
        val right = temporalJoin.getRight.asInstanceOf[StreamPhysicalRel]
        val newLeft = this.satisfyRequiredChangelogMode(left, requiredMode)
        val newRight = this.satisfyRequiredChangelogMode(right, getChangelogModeTraitValue(right))
        // forward left input changes
        val producedMode = getChangelogModeTraitValue(newLeft)
        replaceInputsAndChangelogMode(temporalJoin, List(newLeft, newRight), producedMode)

      case _: StreamExecCalc | _: StreamExecPythonCalc | _: StreamExecCorrelate |
           _: StreamExecPythonCorrelate | _: StreamExecLookupJoin | _: StreamExecExchange |
           _: StreamExecExpand | _: StreamExecMiniBatchAssigner |
           _: StreamExecWatermarkAssigner =>
        // transparent forward requiredMode to children
        val children = visitChildren(rel, requiredMode)
        // they can just forward changes, can't actively satisfy to another changelog mode
        val newInputMode = getChangelogModeTraitValue(children.head)
        replaceInputsAndChangelogMode(rel, children, newInputMode)

      case union: StreamExecUnion =>
        val children = union.getInputs.map { case input: StreamPhysicalRel =>
          val inputMode = getChangelogModeTraitValue(input)
          val requiredInputMode = if (requiredMode.contains(RowKind.UPDATE_BEFORE)) {
            addBeforeImageForUpdates(inputMode)
          } else {
            removeBeforeImageForUpdates(inputMode)
          }
          this.satisfyRequiredChangelogMode(input, requiredInputMode)
        }.toList
        // union can just forward changes, can't actively satisfy to another changelog mode
        val producedMode = ChangelogModeUtils.union(children.map(getChangelogModeTraitValue): _*)
        replaceInputsAndChangelogMode(union, children, producedMode)

      case _: StreamExecDataStreamScan | _: StreamExecTableSourceScan | _: StreamExecValues =>
        convertToRequiredChangelogMode(rel, List(), requiredMode)

      case intermediate: StreamExecIntermediateTableScan =>
        // we can't change the changelog mode of an intermediate source
        intermediate

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported visit for ${rel.getClass.getSimpleName}")
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requestedChildrenMode: ChangelogMode): List[StreamPhysicalRel] = {
      parent.getInputs.map { child =>
        this.satisfyRequiredChangelogMode(child.asInstanceOf[StreamPhysicalRel], requestedChildrenMode)
      }.toList
    }

    private def visitRankOrSortLimit(
        rel: StreamPhysicalRel,
        rankStrategy: RankProcessStrategy,
        requiredMode: ChangelogMode): StreamPhysicalRel = {
      val childMode = getChangelogModeTraitValue(rel.getInput(0))
      val requiredChildMode = if (rankStrategy != RetractStrategy) {
        // doesn't require update_before
        removeBeforeImageForUpdates(childMode)
      } else {
        addBeforeImageForUpdates(childMode)
      }
      val children = visitChildren(rel, requiredChildMode)
      convertToRequiredChangelogMode(rel, children, requiredMode)
    }

    private def convertToRequiredChangelogMode(
      node: StreamPhysicalRel,
      newInputs: List[StreamPhysicalRel],
      requiredMode: ChangelogMode): StreamPhysicalRel = {
      val currentMode = getChangelogModeTraitValue(node)
      val newMode = if (currentMode.equals(requiredMode)) {
        currentMode
      } else {
        val currentModeWithoutBefore = ChangelogModeUtils.removeBeforeImageForUpdates(currentMode)
        if (currentModeWithoutBefore.equals(requiredMode)) {
          requiredMode
        } else {
          val currentModeString = ChangelogModeUtils.stringifyChangelogMode(currentMode)
          val requiredModeString = ChangelogModeUtils.stringifyChangelogMode(requiredMode)
          val plan = FlinkRelOptUtil.toString(node, withRetractTraits = true)
          throw new UnsupportedOperationException(s"Changelog mode [$currentModeString] can't " +
            s"satisfy required mode [$requiredModeString] for now. Plan:\n$plan")
        }
      }
      replaceInputsAndChangelogMode(node, newInputs, newMode)
    }
  }

  // -------------------------------------------------------------------------------------------

  private def getChangelogModeTraitValue(node: RelNode): ChangelogMode = {
    val changelogModeTrait = node.getTraitSet.getTrait(ChangelogModeTraitDef.INSTANCE)
    changelogModeTrait.changelogMode.orNull
  }

  private def replaceInputsAndChangelogMode(
      node: StreamPhysicalRel,
      newInputs: List[StreamPhysicalRel],
      producedMode: ChangelogMode): StreamPhysicalRel = {
    val newTraitSet = node.getTraitSet.plus(new ChangelogModeTrait(Option(producedMode)))
    node.copy(newTraitSet, newInputs).asInstanceOf[StreamPhysicalRel]
  }
}
