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

package org.apache.flink.table.sources.v2;

import java.util.List;
import java.util.Map;

/**
 * .
 */
public interface SupportsPrunePartitions {

	/**
	 * Returns all the partitions of this {@link TableSourceV2}.
	 */
	List<Map<String, String>> getPartitions();

	/**
	 * Applies the remaining partitions to the table source. The {@code remainingPartitions} is
	 * the remaining partitions of {@link #getPartitions()} after partition pruning applied.
	 *
	 * <p>After trying to apply partition pruning, we should return a new {@link TableSourceV2}
	 * instance which holds all pruned-partitions.
	 *
	 * @param remainingPartitions Remaining partitions after partition pruning applied.
	 * @return A new cloned instance of {@link TableSourceV2} holds all pruned-partitions.
	 */
	TableSourceV2 applyPartitionPruning(List<Map<String, String>> remainingPartitions);

}
