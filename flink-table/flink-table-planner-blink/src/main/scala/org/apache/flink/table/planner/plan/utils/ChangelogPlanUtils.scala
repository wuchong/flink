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

package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.volcano.RelSubset
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`.{ChangelogMode, ChangelogModeTrait, ChangelogModeTraitDef}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel
import org.apache.flink.table.planner.plan.rules.physical.stream.ChangelogModeInferRules

import scala.collection.JavaConverters._

object ChangelogPlanUtils {

  val INSERT_ONLY_MODE: ChangelogMode = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.INSERT)
    .build()

  /**
   * Returns the ChangelogMode of this [[RelNode]].
   *
   * Note: this method is depends on [[ChangelogModeTrait]], so must be called after
   * [[ChangelogModeInferRules]] is applied.
   */
  def getChangelogMode(rel: RelNode): ChangelogMode = {
    val changelogModeTrait = rel.getTraitSet.getTrait(ChangelogModeTraitDef.INSTANCE)
    changelogModeTrait.changelogMode.getOrElse(INSERT_ONLY_MODE)
  }

  /**
   * Returns true if the [[RelNode]] will produce delete or update before messages.
   * We call those messages retraction messages, because this method is used to determine
   * whether a user-defined aggregate function requires `retract(...)` method.
   *
   * Note: this method is depends on [[ChangelogModeTrait]], so must be called after
   * [[ChangelogModeInferRules]] is applied.
   */
  def containsRetraction(rel: RelNode): Boolean = {
    val changelogMode = getChangelogMode(rel)
    changelogMode.contains(RowKind.DELETE) || changelogMode.contains(RowKind.UPDATE_BEFORE)
  }

  /**
   * Returns true if the [[RelNode]] will produce update before messages.
   * This method is used to determine whether the runtime operator should
   * emit update before messages.
   *
   * Note: this method is depends on [[ChangelogModeTrait]], so must be called after
   * [[ChangelogModeInferRules]] is applied.
   */
  def containsUpdateBefore(rel: RelNode): Boolean = {
    val changelogMode = getChangelogMode(rel)
    changelogMode.contains(RowKind.UPDATE_BEFORE)
  }

  /**
   * Returns true if the [[RelNode]] will produce deletion messages.
   * This method can be called before [[ChangelogModeInferRules]] is applied.
   */
  def containsDeletion(plan: RelNode): Boolean = {
    val deletionValidator = new DeletionValidator
    deletionValidator.go(plan)

    deletionValidator.containsDeletion
  }

  /**
   * Validates that the plan produces only insert-only changes.
   * This method can be called before [[ChangelogModeInferRules]] is applied.
   */
  def isInsertOnly(plan: RelNode): Boolean = {
    val appendOnlyValidator = new InsertOnlyValidator
    appendOnlyValidator.go(plan)

    appendOnlyValidator.isInsertOnly
  }

  def validateInputStreamIsInsertOnly(node: RelNode, nodeName: String): Unit = {
    val inputsAreInsertOnly = node.getInputs.asScala.forall(input => isInsertOnly(input))
    if (!inputsAreInsertOnly) {
      throw new TableException(
        s"$nodeName only support insert-only input stream, " +
          s"but the input stream contains update changes. \n" +
          s"Note: $nodeName shouldn't follow after a regular aggregation or regular join.")
    }
  }

  // ------------------------------------------------------------------------------------------

  private class DeletionValidator extends RelVisitor {

    var containsDeletion = false

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: StreamPhysicalRel if s.produceDeletions =>
          // if one of the nodes will produce delete, the whole tree will produce delete
          containsDeletion = true
        case hep: HepRelVertex =>
          visit(hep.getCurrentRel, ordinal, parent)   //remove wrapper node
        case rs: RelSubset =>
          visit(rs.getOriginal, ordinal, parent)      //remove wrapper node
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }

  private class InsertOnlyValidator extends RelVisitor {

    var isInsertOnly = true

    override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
      node match {
        case s: StreamPhysicalRel if s.produceUpdates || s.produceDeletions =>
          // if one of the nodes will produce update or delete, the whole tree is not insert-only
          isInsertOnly = false
        case hep: HepRelVertex =>
          visit(hep.getCurrentRel, ordinal, parent)   //remove wrapper node
        case rs: RelSubset =>
          visit(rs.getOriginal, ordinal, parent)      //remove wrapper node
        case _ =>
          super.visit(node, ordinal, parent)
      }
    }
  }

}
