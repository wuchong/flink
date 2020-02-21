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

	/**
	 * Returns whether computed columns can be pushed into the {@link ChangelogReader} or if they
	 * need to be added in a subsequent projection after the source.
	 *
	 * <p>Disabling the computed column push down is only valid for implementations that don't use the
	 * recommended {@link ChangelogRowProducer}.
	 */
	default boolean supportsComputedColumnPushDown() {
		return true;
	}

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Creates the type information describing the internal format of produced {@link ChangelogRow}s.
		 *
		 * <p>Considers computed columns as part of the schema if necessary.
		 */
		TypeInformation<ChangelogRow> createChangelogRowTypeInfo();

		/**
		 * Creates a producer that generates instances of {@link ChangelogRow} during runtime.
		 *
		 * <p>Generates and adds computed columns if necessary.
		 */
		ChangelogRowProducer createChangelogRowProducer();

		/**
		 * Creates the produced data type of the given schema.
		 *
		 * <p>Ignores non-persisted computed columns if necessary.
		 */
		DataType createProducedDataType();

		/**
		 * Creates a runtime data format converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 */
		DataFormatConverter createDataFormatConverter(DataType producedDataType);
	}

	interface ChangelogRowProducer extends FormatConverter {

		/**
		 * Wraps the columns of the given row in internal format into a changelog row of a given kind.
		 *
		 * <p>Generates and adds computed columns if necessary.
		 */
		ChangelogRow wrapInternalRow(ChangelogRow.Kind kind, Object internalRowFormat);
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
