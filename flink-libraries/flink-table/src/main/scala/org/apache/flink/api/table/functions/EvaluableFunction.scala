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
package org.apache.flink.api.table.functions

import java.lang.reflect.{Modifier, Method}
import org.apache.flink.api.table.ValidationException

/**
  * User-defined function has eval methods can extend this trait to reuse the same logic, such as:
  * [[ScalarFunction]] and [[TableFunction]].
  */
trait EvaluableFunction {

  private lazy val evalMethods = checkAndExtractEvalMethods()
  private lazy val signatures = evalMethods.map(_.getParameterTypes)

  /**
    * Extracts evaluation methods and throws a [[ValidationException]] if no implementation
    * can be found.
    */
  private def checkAndExtractEvalMethods(): Array[Method] = {
    val methods = getClass
      .getDeclaredMethods
      .filter { m =>
        val modifiers = m.getModifiers
        m.getName == "eval" && Modifier.isPublic(modifiers) && !Modifier.isAbstract(modifiers)
      }

    if (methods.isEmpty) {
      throw new ValidationException(s"Table function class '$this' does not implement at least " +
                                      s"one method named 'eval' which is public and not abstract.")
    } else {
      methods
    }
  }

  /**
    * Returns all found evaluation methods of the possibly overloaded function.
    */
  private[flink] final def getEvalMethods: Array[Method] = evalMethods

  /**
    * Returns all found signature of the possibly overloaded function.
    */
  private[flink] final def getSignatures: Array[Array[Class[_]]] = signatures

}
