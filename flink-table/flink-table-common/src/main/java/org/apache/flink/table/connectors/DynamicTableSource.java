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

package org.apache.flink.table.connectors;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Source of a dynamic table from an external storage system.
 *
 * <p>A dynamic table source can be seen as a factory that produces concrete runtime implementation. Depending
 * on the specified {@link ReadingAbility}, the planner might apply changes to instances of this class and thus
 * mutates the produced runtime implementation.
 *
 * <p>Use {@link ReadingAbility}s to specify how to read the table.
 */
@PublicEvolving
public interface DynamicTableSource {

	/**
	 * Returns a string that summarizes this source for printing to a console or log.
	 */
	String asSummaryString();
}
