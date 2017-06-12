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
package org.apache.flink.table.runtime.cep

import org.apache.flink.cep.pattern.conditions.IterativeCondition
import org.apache.flink.table.codegen.Compiler
import org.apache.flink.types.Row
import org.slf4j.LoggerFactory

class IterativeConditionRunner(
  name: String,
  code: String)
  extends IterativeCondition[Row]
  with Compiler[IterativeCondition[Row]]{

  val LOG = LoggerFactory.getLogger(this.getClass)

  // IterativeCondition will be serialized as part of state,
  // so make function as transient to avoid ClassNotFound when restore state
  @transient private var function: IterativeCondition[Row] = _

  def init(): Unit = {
    LOG.debug(s"Compiling IterativeCondition: $name \n\n Code:\n$code")
    // FIXME: this is problematic, we can't get user's classloader currently,
    // so it may fail in distributed runtime
    val clazz = compile(Thread.currentThread().getContextClassLoader, name, code)
    LOG.debug("Instantiating FlatMapFunction.")
    function = clazz.newInstance()
  }

  override def filter(
    value: Row,
    ctx: IterativeCondition.Context[Row]): Boolean = {

    if (function == null) {
      init()
    }

    function.filter(value, ctx)
  }
}
