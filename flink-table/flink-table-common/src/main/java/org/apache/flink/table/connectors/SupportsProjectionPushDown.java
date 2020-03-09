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
import org.apache.flink.table.api.TableSchema;

/**
 * Allows to push down (possibly nested) projections into a {@link DynamicTableSource) if it {@link SupportsChangelogReading}.
 */
@PublicEvolving
public interface SupportsProjectionPushDown extends ReadingAbility {

	/**
	 * Apply the (possible nested) projection into the table source to project its output to the
	 * given requiredSchema. Returns the truly remaining schema of the table source.
	 *
	 * Implementation should try its best to prune the unnecessary columns or nested fields, but
	 * it's also fine to do the pruning partially, e.g., a table source may not be able to prune
	 * nested fields, and only prune top-level columns.
	 *
	 * If a table source supports to prune nested fields, then it can apply the requiredSchema
	 * for projection and return it as remaining schema. But if a table source doesn't support
	 * to prune nested fields, it should only prune top-level columns and return a truly remaining
	 * schema (i.e. keeps nested fields).
	 *
	 * @param requiredSchema the required schema (possible nested fields pruned) after projection.
	 * @return the remaining schema after projection.
	 */
	TableSchema applyProjection(TableSchema requiredSchema);
}
