/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformats;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;

/**
 * An interface for row used internally in Flink Table/SQL, which only contains the columns as
 * internal types.
 *
 * <p>A {@link RowData} also contains {@link RowKind} which is a metadata information of
 * this row, not a column of this row. The {@link RowKind} represents the changelog operation
 * kind: INSERT/DELETE/UPDATE_BEFORE/UPDATE_AFTER.
 *
 * <p>TODO: add a list and description for all internal formats
 *
 * <p>There are different implementations depending on the scenario. For example, the binary-orient
 * implementation {@link BinaryRowData} and the object-orient implementation {@link GenericRowData}. All
 * the different implementations have the same binary format after serialization.
 *
 * <p>{@code BaseRow}s are influenced by Apache Spark InternalRows.
 */
@PublicEvolving
public interface RowData extends TypedGetters, Serializable {

	/**
	 * Get the number of fields in the BaseRow.
	 *
	 * @return The number of fields in the BaseRow.
	 */
	int getArity();

	/**
	 * Gets the changelog kind of this row, it is a metadata of this row, not a column of this row.
	 */
	RowKind getChangelogKind();

	/**
	 * Sets the changelog kind of this row, it is a metadata of this row, not a column of this row.
	 */
	void setChangelogKind(RowKind kind);

	// ------------------------------------------------------------------------------------------

	static Object get(RowData row, int ordinal, LogicalType type) {
		return TypedGetters.get(row, ordinal, type);
	}
}
