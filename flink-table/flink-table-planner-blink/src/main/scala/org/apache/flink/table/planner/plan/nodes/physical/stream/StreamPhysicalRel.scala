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

import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.planner.plan.`trait`.ChangelogMode

/**
  * Base class for stream physical relational expression.
  */
trait StreamPhysicalRel extends FlinkPhysicalRel {

  // -------------------------------------------------------------------------------------------
  // (Option#1) Interfaces used to infer ChangelogMode trait for nodes
  // -------------------------------------------------------------------------------------------

  def supportChangelogMode(inputChangelogModes: Array[ChangelogMode]): Boolean

  def producedChangelogMode(inputChangelogModes: Array[ChangelogMode]): ChangelogMode

  def consumedChangelogMode(
    inputOrdinal: Int,
    inputMode: ChangelogMode,
    expectedOutputMode: ChangelogMode): ChangelogMode

  // -------------------------------------------------------------------------------------------
  // (Option#2) Interfaces used to infer ChangelogMode trait for nodes
  // -------------------------------------------------------------------------------------------

  /**
    * Whether the [[StreamPhysicalRel]] produces update and delete changes.
    */
  def produceUpdates: Boolean

  def produceDeletions: Boolean

  def requestBeforeImageOfUpdates(input: RelNode): Boolean

  /**
   * Whether the [[StreamPhysicalRel]] forwards changes (insert/delete/update). Some nodes don't
   * produce new changes but simply forward changes without changing the
   * [[org.apache.flink.table.dataformat.RowKind]] of each rows, e.g. Calc, Correlate. Some nodes
   * consumes received changes and produce new changes, e.g. Aggregate, Join.
   */
  def forwardChanges: Boolean

//  /**
//    * Whether the [[StreamPhysicalRel]] requires retraction messages or not.
//    */
//  def needsUpdatesAsRetraction(input: RelNode): Boolean
//
//  /**
//    * Whether the [[StreamPhysicalRel]] consumes retraction messages instead of forwarding them.
//    * The node might or might not produce new retraction messages.
//    */
//  def consumesRetractions: Boolean
//
//  /**
//    * Whether the [[StreamPhysicalRel]] produces retraction messages.
//    */
//  def producesRetractions: Boolean

  // -------------------------------------------------------------------------------------------
  // Interfaces used to infer MiniBatchInterval trait for nodes
  // -------------------------------------------------------------------------------------------

  /**
    * Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic.
    */
  def requireWatermark: Boolean
}
