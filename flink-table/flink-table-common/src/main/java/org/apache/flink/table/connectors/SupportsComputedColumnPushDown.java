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

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public interface SupportsComputedColumnPushDown extends ReadingAbility {

	default boolean supportsComputedColumnPushDown() {
		return true;
	}

	DynamicTableSource applyComputedColumn(ChangelogRowConverter converter);

	/**
	 * The converter that converts the produced ChangelogRow of the physical fields of the source
	 * into a new ChangelogRow with the additional push-downed computed columns.
	 *
	 * <p>The {@link #getProducedType()} contains the computed columns as part of the schema.
	 *1
	 * <p>For example, if we have a table defined as
	 * CREATE TABLE t1 (str STRING, ts AS TO_TIMESTAMP(str), cnt INT, cnt2 AS cnt +1).
	 * Then this converter can help to convert a ChangelogRow(str, cnt) into ChangelogRow(str, ts, cnt, cnt2).
	 * The {@link #getProducedType()} of this converter is {@code ChangelogRow<STRING, TIMESTAMP, INT, INT>}
	 * which contains the computed column as part of the schema.
	 */
	interface ChangelogRowConverter extends FormatConverter, ResultTypeQueryable<ChangelogRow> {

		/**
		 * Wraps the columns of the given row in internal format into a changelog row of a given kind.
		 *
		 * <p>Generates and adds computed columns if necessary.
		 */
		ChangelogRow convert(ChangelogRow changelogRow);

	}
}
