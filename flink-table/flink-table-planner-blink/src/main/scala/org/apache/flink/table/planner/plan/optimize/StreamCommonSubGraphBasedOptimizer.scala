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

package org.apache.flink.table.planner.plan.optimize

import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rex.RexBuilder
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.planner.calcite.{FlinkContext, SqlExprToRexConverterFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamExecDataStreamScan, StreamExecIntermediateTableScan, StreamPhysicalRel}
import org.apache.flink.table.planner.plan.optimize.program.{FlinkStreamProgram, StreamOptimizeContext}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.{ChangelogPlanUtils, FlinkRelOptUtil}
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.utils.TableConfigUtils
import org.apache.flink.table.planner.utils.TableConfigUtils.getMillisecondFromConfigDuration
import org.apache.flink.table.sinks.RetractStreamTableSink
import org.apache.flink.util.Preconditions

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

/**
  * A [[CommonSubGraphBasedOptimizer]] for Stream.
  */
class StreamCommonSubGraphBasedOptimizer(planner: StreamPlanner)
  extends CommonSubGraphBasedOptimizer {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    val config = planner.getTableConfig
    // build RelNodeBlock plan
    val sinkBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(roots, config)
    // infer updateAsRetraction property for sink block
    sinkBlocks.foreach { sinkBlock =>
      val retractionFromRoot = sinkBlock.outputNode match {
        case n: Sink =>
          n.sink match {
            case _: RetractStreamTableSink[_] => true
            case s: DataStreamTableSink[_] => s.requestBeforeImageOfUpdate
            case _ => false
          }
        case o =>
          o.getTraitSet.getTrait(SendBeforeImageForUpdatesTraitDef.INSTANCE).sendBeforeImageForUpdates
      }
      sinkBlock.setUpdateAsRetraction(retractionFromRoot)
      val miniBatchInterval: MiniBatchInterval = if (config.getConfiguration.getBoolean(
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)) {
        val miniBatchLatency = getMillisecondFromConfigDuration(config,
          ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY)
        Preconditions.checkArgument(miniBatchLatency > 0,
          "MiniBatch Latency must be greater than 0 ms.", null)
        MiniBatchInterval(miniBatchLatency, MiniBatchMode.ProcTime)
      }  else {
        MiniBatchIntervalTrait.NONE.getMiniBatchInterval
      }
      sinkBlock.setMiniBatchInterval(miniBatchInterval)
    }

    if (sinkBlocks.size == 1) {
      // If there is only one sink block, the given relational expressions are a simple tree
      // (only one root), not a dag. So many operations (e.g. `infer updateAsRetraction property`,
      // `propagate updateAsRetraction property`) can be omitted to save optimization time.
      val block = sinkBlocks.head
      val optimizedTree = optimizeTree(
        block.getPlan,
        block.requestBeforeImageOfUpdate,
        block.getMiniBatchInterval,
        isSinkBlock = true)
      block.setOptimizedPlan(optimizedTree)
      return sinkBlocks
    }

    // infer updateAsRetraction property and miniBatchInterval property for all input blocks
    sinkBlocks.foreach(b => inferTraits(
      b, b.requestBeforeImageOfUpdate, b.getMiniBatchInterval, isSinkBlock = true))
    // propagate updateAsRetraction property and miniBatchInterval property to all input blocks
    sinkBlocks.foreach(propagateTraits(_, isSinkBlock = true))
    // clear the intermediate result
    sinkBlocks.foreach(resetIntermediateResult)
    // optimize recursively RelNodeBlock
    sinkBlocks.foreach(b => optimizeBlock(b, isSinkBlock = true))
    sinkBlocks
  }

  private def optimizeBlock(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {
    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          optimizeBlock(child, isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case s: Sink =>
        require(isSinkBlock)
        val optimizedTree = optimizeTree(
          s,
          requestUpdateBefore = block.requestBeforeImageOfUpdate,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = true)
        block.setOptimizedPlan(optimizedTree)

      case o =>
        val optimizedPlan = optimizeTree(
          o,
          requestUpdateBefore = block.requestBeforeImageOfUpdate,
          miniBatchInterval = block.getMiniBatchInterval,
          isSinkBlock = isSinkBlock)
        val changelogMode = ChangelogPlanUtils.getChangelogMode(optimizedPlan)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(name, optimizedPlan, changelogMode)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The root node of the relational expression tree.
    * @param requestUpdateBefore True if request before image of updates
    * @param miniBatchInterval mini-batch interval of the block.
    * @param isSinkBlock True if the given block is sink block.
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(
      relNode: RelNode,
      requestUpdateBefore: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): RelNode = {

    val config = planner.getTableConfig
    val calciteConfig = TableConfigUtils.getCalciteConfig(config)
    val programs = calciteConfig.getStreamProgram
      .getOrElse(FlinkStreamProgram.buildProgram(config.getConfiguration))
    Preconditions.checkNotNull(programs)

    val context = relNode.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])

    programs.optimize(relNode, new StreamOptimizeContext() {

      override def getTableConfig: TableConfig = config

      override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

      override def getCatalogManager: CatalogManager = planner.catalogManager

      override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory =
        context.getSqlExprToRexConverterFactory

      override def getRexBuilder: RexBuilder = planner.getRelBuilder.getRexBuilder

      def getMiniBatchInterval: MiniBatchInterval = miniBatchInterval

      override def needFinalTimeIndicatorConversion: Boolean = true

      override def requestBeforeImageOfUpdate: Boolean = requestUpdateBefore
    })
  }

  /**
    * Infer UpdateAsRetraction property and MiniBatchInterval property for each block.
    * NOTES: this method should not change the original RelNode tree.
    *
    * @param block              The [[RelNodeBlock]] instance.
    * @param retractionFromRoot Whether the sink need update as retraction messages.
    * @param miniBatchInterval  mini-batch interval of the block.
    * @param isSinkBlock        True if the given block is sink block.
    */
  private def inferTraits(
      block: RelNodeBlock,
      retractionFromRoot: Boolean,
      miniBatchInterval: MiniBatchInterval,
      isSinkBlock: Boolean): Unit = {

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.isEmpty) {
          inferTraits(
            child,
            retractionFromRoot = false,
            miniBatchInterval = MiniBatchInterval.NONE,
            isSinkBlock = false)
        }
    }

    val blockLogicalPlan = block.getPlan
    blockLogicalPlan match {
      case n: Sink =>
        require(isSinkBlock)
        val optimizedPlan = optimizeTree(
          n, retractionFromRoot, miniBatchInterval, isSinkBlock = true)
        block.setOptimizedPlan(optimizedPlan)

      case o =>
        val optimizedPlan = optimizeTree(
          o, retractionFromRoot, miniBatchInterval, isSinkBlock = isSinkBlock)
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable = createIntermediateRelTable(name, optimizedPlan,
          ChangelogPlanUtils.INSERT_ONLY_MODE)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
        block.setOptimizedPlan(optimizedPlan)
    }
  }

  /**
    * Propagate updateAsRetraction property and miniBatchInterval property to all input blocks.
    *
    * @param block The [[RelNodeBlock]] instance.
    * @param isSinkBlock True if the given block is sink block.
    */
  private def propagateTraits(block: RelNodeBlock, isSinkBlock: Boolean): Unit = {

    // process current block
    def shipTraits(
        rel: RelNode,
        requestBeforeImageOfUpdate: Boolean,
        miniBatchInterval: MiniBatchInterval): Unit = {
      rel match {
        case _: StreamExecDataStreamScan | _: StreamExecIntermediateTableScan =>
          val scan = rel.asInstanceOf[TableScan]
          val emitUpdateBeforeTrait = scan.getTraitSet.getTrait(SendBeforeImageForUpdatesTraitDef.INSTANCE)
          val miniBatchIntervalTrait = scan.getTraitSet.getTrait(MiniBatchIntervalTraitDef.INSTANCE)
          val tableName = scan.getTable.getQualifiedName.mkString(".")
          val inputBlocks = block.children.filter(b => tableName.equals(b.getOutputTableName))
          Preconditions.checkArgument(inputBlocks.size <= 1)
          if (inputBlocks.size == 1) {
            val mergedInterval = if (isSinkBlock) {
              // traits of sinkBlock have already been
              // initialized before first round of optimization.
              miniBatchIntervalTrait.getMiniBatchInterval
            } else {
              FlinkRelOptUtil.mergeMiniBatchInterval(
                miniBatchIntervalTrait.getMiniBatchInterval, miniBatchInterval)
            }
            val newInterval = FlinkRelOptUtil.mergeMiniBatchInterval(
              inputBlocks.head.getMiniBatchInterval,mergedInterval)
            inputBlocks.head.setMiniBatchInterval(newInterval)

            if (emitUpdateBeforeTrait.sendBeforeImageForUpdates || requestBeforeImageOfUpdate) {
              inputBlocks.head.setUpdateAsRetraction(true)
            }
          }
        case ser: StreamPhysicalRel => ser.getInputs.foreach { e =>
          if (ser.requestBeforeImageOfUpdates(e) ||
              (requestBeforeImageOfUpdate && ser.forwardChanges)) {
            shipTraits(e, requestBeforeImageOfUpdate = true, miniBatchInterval)
          } else {
            shipTraits(e, requestBeforeImageOfUpdate = false, miniBatchInterval)
          }
        }
      }
    }

    shipTraits(block.getOptimizedPlan, block.requestBeforeImageOfUpdate, block.getMiniBatchInterval)
    block.children.foreach(propagateTraits(_, isSinkBlock = false))
  }

  /**
    * Reset the intermediate result including newOutputNode and outputTableName
    *
    * @param block the [[RelNodeBlock]] instance.
    */
  private def resetIntermediateResult(block: RelNodeBlock): Unit = {
    block.setNewOutputNode(null)
    block.setOutputTableName(null)

    block.children.foreach {
      child =>
        if (child.getNewOutputNode.nonEmpty) {
          resetIntermediateResult(child)
        }
    }
  }

  private def createIntermediateRelTable(
      name: String,
      relNode: RelNode,
      changelogMode: ChangelogMode): IntermediateRelTable = {
    val uniqueKeys = getUniqueKeys(relNode)
    val monotonicity = FlinkRelMetadataQuery
      .reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery)
      .getRelModifiedMonotonicity(relNode)
    val statistic = FlinkStatistic.builder()
      .uniqueKeys(uniqueKeys)
      .relModifiedMonotonicity(monotonicity)
      .build()
    new IntermediateRelTable(Collections.singletonList(name), relNode, changelogMode, statistic)
  }

  private def getUniqueKeys(relNode: RelNode): util.Set[_ <: util.Set[String]] = {
    val rowType = relNode.getRowType
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(planner.getRelBuilder.getCluster.getMetadataQuery)
    val uniqueKeys = fmq.getUniqueKeys(relNode)
    if (uniqueKeys != null) {
      uniqueKeys.filter(_.nonEmpty).map { uniqueKey =>
        val keys = new util.HashSet[String]()
        uniqueKey.asList().foreach { idx =>
          keys.add(rowType.getFieldNames.get(idx))
        }
        keys
      }
    } else {
      null
    }
  }

}
