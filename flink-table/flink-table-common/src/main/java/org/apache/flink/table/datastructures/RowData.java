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

package org.apache.flink.table.datastructures;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.Serializable;

/**
 * {@link RowData} is an internal data structure representing data of {@link RowType}
 * in Flink Table/SQL, which only contains columns of the internal data structures.
 *
 * <p>A {@link RowData} also contains a {@link RowKind} which represents the kind of row in
 * a changelog. The {@link RowKind} is just a metadata information of row, not a column.
 *
 * <p>{@link RowData} has different implementations which are designed for different scenarios.
 * For example, the binary-orient implementation {@link BinaryRowData} is backed by
 * {@link MemorySegment} instead of Object to reduce serialization/deserialization cost.
 * The object-orient implementation {@link GenericRowData} is backed by an array of Object
 * which is easy to construct and efficient to update.
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
