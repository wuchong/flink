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

import org.apache.calcite.plan.{RelOptPlanner, RelTraitDef}
import org.apache.calcite.rel.RelNode

/**
 * Definition of the [[ChangelogModeTrait]].
 */
class ChangelogModeTraitDef extends RelTraitDef[ChangelogModeTrait] {

  override def getTraitClass: Class[ChangelogModeTrait] = classOf[ChangelogModeTrait]

  override def getSimpleName: String = this.getClass.getSimpleName

  override def convert(
      planner: RelOptPlanner,
      rel: RelNode,
      toTrait: ChangelogModeTrait,
      allowInfiniteCostConverters: Boolean): RelNode = {
    rel.copy(rel.getTraitSet.plus(toTrait), rel.getInputs)
  }

  override def canConvert(
    planner: RelOptPlanner,
    fromTrait: ChangelogModeTrait,
    toTrait: ChangelogModeTrait): Boolean = true

  override def getDefault: ChangelogModeTrait = ChangelogModeTrait.DEFAULT
}

object ChangelogModeTraitDef {
  val INSTANCE = new ChangelogModeTraitDef
}