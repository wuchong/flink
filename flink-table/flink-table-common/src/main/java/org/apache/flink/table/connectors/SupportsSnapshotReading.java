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
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/**
 * A {@link ReadingAbility} that reads a sequence of rows from an external storage system
 * as a snapshot of the table at the current processing time.
 *
 * The {@link SupportsSnapshotReading} always work in {@link ChangelogMode#insertOnly()} mode,
 * generates insert-only records and is bounded.
 */
@PublicEvolving
public interface SupportsSnapshotReading extends ReadingAbility {

	/**
	 * Returns the actual implementation for reading the data.
	 */
	SnapshotReader getSnapshotReader(Context context);

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
		 */
		DataFormatConverter createDataFormatConverter(DataType producedDataType);
	}

	interface DataFormatConverter extends FormatConverter {

		/**
		 * Converts the given object into an internal data format.
		 */
		@Nullable Object toInternal(@Nullable Object externalFormat);
	}

	interface SnapshotReader {
		// marker interface
	}
}
