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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.dataformat.RowKind
import org.apache.flink.table.planner.plan.`trait`.ChangelogMode

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object ChangelogModeUtils {

  val ALL_CHANGES: ChangelogMode = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.INSERT)
    .addContainedKind(RowKind.UPDATE_BEFORE)
    .addContainedKind(RowKind.UPDATE_AFTER)
    .addContainedKind(RowKind.DELETE)
    .build()

  val INSERT_ONLY: ChangelogMode = ChangelogMode.newBuilder()
    .addContainedKind(RowKind.INSERT)
    .build()

  def isInsertOnly(changelogMode: ChangelogMode): Boolean = {
    changelogMode.containsOnly(RowKind.INSERT)
  }

  def addBeforeImageForUpdates(changelogMode: ChangelogMode): ChangelogMode = {
    // request update_before if there is update_after
    if (changelogMode.contains(RowKind.UPDATE_AFTER) &&
        !changelogMode.contains(RowKind.UPDATE_BEFORE)) {
      val resultBuilder = ChangelogMode.newBuilder().addContainedKind(RowKind.UPDATE_BEFORE)
      changelogMode.getContainedKinds.asScala.foreach(resultBuilder.addContainedKind)
      return resultBuilder.build()
    }
    changelogMode
  }

  def removeBeforeImageForUpdates(changelogMode: ChangelogMode): ChangelogMode = {
    if (changelogMode.contains(RowKind.UPDATE_AFTER) &&
        changelogMode.contains(RowKind.UPDATE_BEFORE)) {
      val resultBuilder = ChangelogMode.newBuilder()
      changelogMode.getContainedKinds.asScala.foreach { mode =>
        if (mode != RowKind.UPDATE_BEFORE) {
          resultBuilder.addContainedKind(mode)
        }
      }
      return resultBuilder.build()
    }
    changelogMode
  }

  def stringifyChangelogMode(mode: ChangelogMode): String = {
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
  }

}
