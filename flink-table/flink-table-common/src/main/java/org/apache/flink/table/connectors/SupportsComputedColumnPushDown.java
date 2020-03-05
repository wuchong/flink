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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dataformats.BaseRow;

/**
 * Allows to push down computed columns into a {@link DynamicTableSource) if it {@link SupportsChangelogReading}.
 */
@PublicEvolving
public interface SupportsComputedColumnPushDown extends SupportsChangelogReading {

	default boolean supportsComputedColumnPushDown() {
		return true;
	}

	/**
	 * Provides a converter that converts the produced {@link BaseRow} containing the physical
	 * fields of the external system into a new {@link BaseRow} with push-downed computed columns.
	 *
	 * <p>For example, in case of {@code CREATE TABLE t (s STRING, ts AS TO_TIMESTAMP(str), i INT, i2 AS i + 1)},
	 * the converter will convert a {@code ChangelogRow(s, i)} to {@code ChangelogRow(s, ts, i, i2)}.
	 *
	 * <p>Note: Use {@link TableSchema#toRowDataType()} instead of {@link TableSchema#toProducedRowDataType()}
	 * for describing the final output data type when create a {@link TypeInformation}.
	 */
	void applyComputedColumn(ComputedColumnConverter converter);

	/**
	 * Generates and adds computed columns to a {@link BaseRow} if necessary.
	 */
	interface ComputedColumnConverter extends FormatConverter {

		/**
		 * Generates and adds computed columns to a {@link BaseRow} if necessary.
		 */
		BaseRow convert(BaseRow changelogRow);

	}
}
