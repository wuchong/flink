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

package org.apache.flink.table.sinks;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * An abstract class with trait about partitionable table sink. This is mainly used for
 * static partitions. For sql statement:
 * <pre>
 * <code>
 * INSERT INTO A PARTITION(a='ab', b='cd') select c, d from B
 * </code>
 * </pre>
 * We Assume the A has partition columns as &lt;a&gt;, &lt;b&gt;, &lt;c&gt;.
 * The columns &lt;a&gt; and &lt;b&gt; are called static partition columns, while c is called
 * dynamic partition column.
 *
 * <p>Note: Current class implementation don't support partition pruning which means constant
 * partition columns will still be kept in result row.
 */
public interface PartitionableTableSink {

	/**
	 * Check if the table is partitioned or not.
	 *
	 * @return true if the table is partitioned; otherwise, false
	 */
	boolean isPartitioned();

	/**
	 * Get the partition keys of the table. This will be an empty set if the table is not partitioned.
	 *
	 * @return partition keys of the table
	 */
	List<String> getPartitionKeys();

	/**
	 * Sets the static partitions into the {@link TableSink}.
	 * @param partitions mapping from static partition column names to string literal values.
	 *                      String literals will be quoted using {@code '}, for example,
	 *                      value {@code abc} will be stored as {@code 'abc'} with quotes around.
	 */
	void setStaticPartitions(LinkedHashMap<String, String> partitions);

	/**
	 * If true, all records would be sort with partition fields before output, for some sinks, this
	 * can be used to reduce the partition writers, that means the sink will accept data
	 * one partition at a time.
	 *
	 * <p>A sink should consider whether to override this especially when it needs buffer
	 * data before writing.
	 *
	 * <p>Notes:
	 * 1. If returns true, the output data will be sorted <strong>locally</strong> after partitioning.
	 * 2. Default returns true, if the table is partitioned.
	 */
	default boolean sortLocalPartition() {
		return isPartitioned();
	}
}
