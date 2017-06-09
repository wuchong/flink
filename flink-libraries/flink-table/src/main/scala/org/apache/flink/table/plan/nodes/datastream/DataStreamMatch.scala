package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.rex.RexNode
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow


/**
  * Flink DataStream RelNode for LogicalMatch
  */
class DataStreamMatch(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    pattern: RexNode,
    strictStart: Boolean,
    strictEnd: Boolean,
    patternDefinitions: util.Map[String, RexNode],
    schema: RowSchema,
    inputSchema: RowSchema)
  extends SingleRel(cluster, traitSet, input)
  with DataStreamRel {

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new DataStreamMatch(
      cluster,
      traitSet,
      inputs.get(0),
      pattern,
      strictStart,
      strictEnd,
      patternDefinitions,
      schema,
      inputSchema)
  }

  override def toString: String = {
    "Match"
  }


  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {

    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    // TODO

    null
  }


}
