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

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

class PatternSelectFunctionRunner(firstPatternName: String)
  extends PatternSelectFunction[Row, CRow] {

  private var outCRow: CRow = _

  override def select(pattern: util.Map[String, util.List[Row]]): CRow = {
    if (outCRow == null) {
      outCRow = new CRow(null, true)
    }
    val list: util.List[Row] = pattern.get(firstPatternName)
    outCRow.row = list.get(list.size() - 1) // get the last one
    outCRow
  }
}
