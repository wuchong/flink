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

package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.StreamFilter
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.`trait`.ChangelogMode
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.ChangelogModeUtils
import org.apache.flink.table.runtime.operators.changelog.DropUpdateBeforeFunction
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo

import java.util

import scala.collection.JavaConversions._

class StreamExecDropUpdateBefore(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def supportChangelogMode(inputChangelogModes: Array[ChangelogMode]): Boolean = true

  override def producedChangelogMode(inputChangelogModes: Array[ChangelogMode]): ChangelogMode = {
    ChangelogModeUtils.removeBeforeImageForUpdates(inputChangelogModes.head)
  }

  override def consumedChangelogMode(
      inputOrdinal: Int,
      inputMode: ChangelogMode,
      expectedOutputMode: ChangelogMode): ChangelogMode = inputMode

  override def produceUpdates: Boolean = false

  override def produceDeletions: Boolean = false

  override def requestBeforeImageOfUpdates(input: RelNode): Boolean = false

  override def forwardChanges: Boolean = true

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      inputs: util.List[RelNode]): RelNode = {
    new StreamExecDropUpdateBefore(cluster, traitSet, inputs.get(0))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[BaseRow] = {

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]
    val rowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val operator = new StreamFilter(new DropUpdateBeforeFunction())
    new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      rowTypeInfo,
      inputTransform.getParallelism
    )
  }

}
