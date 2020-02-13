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

import org.apache.flink.table.types.DataType;

/**
 * .
 */
public interface TableSourceV2 {

	/**
	 * Returns the DynamicTableType of the source, it can either be {@code Table}
	 * representation or {@code Stream} representation.
	 */
	DynamicTableType getDynamicTableType();

	/**
	 * Returns true if this is a bounded source, false if this is an unbounded source.
	 *
	 * Note: If {@link DynamicTableType} of the source is {@link DynamicTableType#TABLE},
	 * it must be bounded.
	 */
	boolean isBounded();

	/**
	 * Returns the {@link ChangeMode} that the source works in. The {@link ChangeMode} defines
	 * what messages with different {@link org.apache.flink.table.dataformat.ChangeType} can appear
	 * in the produced stream.
	 *
	 * Note: If {@link DynamicTableType} of the source is {@link DynamicTableType#TABLE},
	 * it must be {@link ChangeMode#INSERT_ONLY}
	 */
	ChangeMode getChangeMode();

	/**
	 * Returns the {@link DataType} for the produced data of the {@link TableSourceV2}.
	 *
	 * @return The data type of the returned {@code DataSet} or {@code DataStream}.
	 */
	DataType getProduceDataType();

	DataReaderProvider<?> createDataReaderProvider();
}
