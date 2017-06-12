package org.apache.flink.table.runtime

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

class CRowIterativeConditionRunner(
    name: String,
    code: String)
  extends IterativeCondition[CRow]
  with Compiler[IterativeCondition[Row]] {

  val LOG = LoggerFactory.getLogger(this.getClass)

  private var function: IterativeCondition[Row] = _

  override def filter(
    in: CRow,
    ctx: IterativeCondition.Context[CRow])
  : Boolean = {
    if (function == null) {
      initFunction()
    }

    function.filter(
      in.row,
      ctx.asInstanceOf[IterativeCondition.Context[Row]])
  }

  def initFunction(): Unit = {
    LOG.debug(s"Compiling RichIterativeCondition: $name \n\n Code:\n$code")
    val clazz = compile(this.getClass.getClassLoader, name, code)
    LOG.debug("Instantiating RichIterativeCondition.")
    function = clazz.newInstance()
  }
}
