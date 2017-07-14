///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.flink.table.functions
//
//import java.util.{ArrayList => JArrayList, List => JList}
//
//import org.apache.flink.util.Collector
//
//abstract class OperationFunction[T, ACC] extends AggregateFunction[JList[T], ACC] {
//
//  override final def getValue(accumulator: ACC): JList[T] = {
//    val result = new JArrayList[T]()
//
//  }
//
//  /**
//    * The code generated collector used to emit row.
//    */
//  private var collector: Collector[T] = _
//
//  /**
//    * Emit an output row.
//    *
//    * @param row the output row
//    */
//  protected def collect(row: T): Unit = {
//    collector.collect(row)
//  }
//
//
//}
