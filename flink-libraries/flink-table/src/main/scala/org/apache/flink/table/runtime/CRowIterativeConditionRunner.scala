package org.apache.flink.table.runtime

import org.apache.flink.api.common.functions.util.FunctionUtils
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, RichIterativeCondition}
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

class CRowIterativeConditionRunner(
    name: String,
    code: String)
  extends RichIterativeCondition[CRow]
  with Compiler[RichIterativeCondition[Row]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var function: RichIterativeCondition[Row] = _

  override def open(parameters: Configuration): Unit = {
    LOG.debug(s"Compiling RichIterativeCondition: $name \n\n Code:\n$code")
    val clazz = compile(getRuntimeContext.getUserCodeClassLoader, name, code)
    LOG.debug("Instantiating RichIterativeCondition.")
    function = clazz.newInstance()
    FunctionUtils.setFunctionRuntimeContext(function, getRuntimeContext)
    FunctionUtils.openFunction(function, parameters)
  }

  override def filter(
    in: CRow,
    ctx: IterativeCondition.Context[CRow])
  : Boolean = {
    function.filter(
      in.row,
      ctx.asInstanceOf[IterativeCondition.Context[Row]])
  }

  override def close(): Unit = {
    FunctionUtils.closeFunction(function)
  }
}
