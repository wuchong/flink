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
package org.apache.flink.api.table.expressions.utils

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.functions.TableFunction
import org.apache.flink.api.table.typeutils.RowTypeInfo


case class SimpleUser(name: String, age: Int)

object TableFunc0 extends TableFunction[SimpleUser] {
  // make sure input element's format is "<string>#<int>"
  def eval(user: String): Unit = {
    if (user.contains("#")) {
      val splits = user.split("#")
      collect(SimpleUser(splits(0), splits(1).toInt))
    }
  }
}

object TableFunc1 extends TableFunction[String] {
  def eval(str: String): Unit = {
    if (str.contains("#")){
      str.split("#").foreach(collect)
    }
  }

  def eval(str: String, prefix: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach(s => collect(prefix + s))
    }
  }
}


object TableFunc2 extends TableFunction[Row] {

  def eval(str: String): Unit = {
    if (str.contains("#")) {
      str.split("#").foreach({ s =>
        val row = new Row(2)
        row.setField(0, s)
        row.setField(1, s.length)
        collect(row)
      })
    }
  }

  override def getResultType: TypeInformation[Row] = {
    new RowTypeInfo(Seq(BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO))
  }
}
