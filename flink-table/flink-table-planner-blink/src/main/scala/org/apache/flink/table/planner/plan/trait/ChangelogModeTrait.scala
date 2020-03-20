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

package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.plan.{RelOptPlanner, RelTrait, RelTraitDef}
import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils

import scala.collection.mutable.ArrayBuffer

/**
 * Tracks the [[ChangelogMode]] of a [[org.apache.calcite.rel.RelNode]]
 */
class ChangelogModeTrait(val changelogMode: Option[ChangelogMode]) extends RelTrait {

  override def getTraitDef: RelTraitDef[_ <: RelTrait] = ChangelogModeTraitDef.INSTANCE

  override def satisfies(`trait`: RelTrait): Boolean = this.equals(`trait`)

  override def register(planner: RelOptPlanner): Unit = {}

  override def hashCode(): Int = changelogMode.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case t: ChangelogModeTrait => this.changelogMode.equals(t.changelogMode)
    case _ => false
  }

  override def toString: String = changelogMode match {
    case Some(mode) =>
      val kinds = new ArrayBuffer[String]
      if (mode.contains(RowKind.INSERT)) {
        kinds += "I"
      }
      if (mode.contains(RowKind.UPDATE_BEFORE)) {
        kinds += "UB"
      }
      if (mode.contains(RowKind.UPDATE_AFTER)) {
        kinds += "UA"
      }
      if (mode.contains(RowKind.DELETE)) {
        kinds += "D"
      }
      kinds.mkString(",")
    case None => "NONE"
  }
}

object ChangelogModeTrait {
  val DEFAULT = new ChangelogModeTrait(None)
}
