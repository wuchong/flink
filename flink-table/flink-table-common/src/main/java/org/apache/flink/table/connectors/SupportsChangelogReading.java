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
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/**
 * A {@link ReadingAbility} that reads a changelog of rows from an external storage system.
 */
@PublicEvolving
public interface SupportsChangelogReading extends ReadingAbility {

	/**
	 * Returns what kind of changes are produced by this reader.
	 *
	 * @see ChangelogRow.Kind
	 */
	ChangelogMode getChangelogMode();

	/**
	 * Returns the actual implementation for reading the data.
	 */
	ChangelogReader getChangelogReader(Context context);

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Returns the user code class loader.
		 */
		ClassLoader getUserClassLoader();

		/**
		 * Creates type information describing the internal format of the given {@link DataType}.
		 */
		TypeInformation<?> createTypeInformation(DataType producedDataType);

		/**
		 * Creates a runtime data format converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 *
		 * <p>Note: This converter is only applicable for the top-level record. The planner will validate
		 * that this method is only called with a row data type that corresponds to {@link TableSchema}.
		 */
		ChangelogRowConverter createChangelogRowConverter(DataType producedDataType);

		/**
		 * Creates a runtime data format converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 */
		DataFormatConverter createDataFormatConverter(DataType producedDataType);
	}

	/**
	 * Converter for creating top-level rows that describe the kind of change.
	 */
	interface ChangelogRowConverter extends FormatConverter {

		/**
		 * Converts the given external object into a top-level row using an internal row data format.
		 */
		ChangelogRow toChangelogRow(ChangelogRow.Kind kind, Object externalFormat);
	}

	interface DataFormatConverter extends FormatConverter {

		/**
		 * Converts the given object into an internal data format.
		 */
		@Nullable Object toInternal(@Nullable Object externalFormat);
	}

	interface ChangelogReader {

		/**
		 * Whether the data is bounded or not.
		 */
		boolean isBounded();
	}
}
