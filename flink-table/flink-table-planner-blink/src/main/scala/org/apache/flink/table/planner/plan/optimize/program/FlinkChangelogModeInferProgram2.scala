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
import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets

class FlinkChangelogModeInferProgram2 extends FlinkOptimizeProgram[StreamOptimizeContext] {

  val program: FlinkGroupProgram[StreamOptimizeContext] = FlinkGroupProgramBuilder
    .newBuilder[StreamOptimizeContext]
    .addProgram(
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkStreamRuleSets.CHANGELOG_MODE_INITIAL_RULES)
        .build(), "changelog mode propagate rules")
    .addProgram(new FlinkChangelogModeTraitInitProgram, "changelog mode root init")
    .addProgram(
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.TOP_DOWN)
        .add(FlinkStreamRuleSets.CHANGELOG_MODE_FINAL_RULES)
        .build(), "changelog mode final rules")
    .build()

  override def optimize(
      root: RelNode,
      context: StreamOptimizeContext): RelNode = {
    program.optimize(root, context)
  }
}
