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

package org.apache.flink.table.connector.format;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;

/**
 * TODO
 * A {@link Format} for a {@link ScanTableSource}.
 *
 * @param <I> type of runtime format reader interface needed by the table source
 */
@PublicEvolving
public interface FormatReaderProvider<I> {

	/**
	 * Creates runtime implementation that is configured to produce data of the given data type.
	 */
	I createFormatReader(DynamicTableSource.Context context, DataType producedDataType);

	/**
	 * Returns the set of changes that a connector (and transitively the planner) can expect during
	 * runtime.
	 */
	ChangelogMode getChangelogMode();
}
