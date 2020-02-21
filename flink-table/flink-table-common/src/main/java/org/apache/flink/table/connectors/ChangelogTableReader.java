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
 * A {@link TableReader} that reads a changelog of rows from an external storage system.
 */
@PublicEvolving
public interface ChangelogTableReader extends TableReader {

	/**
	 * Whether the data is bounded or not.
	 */
	boolean isBounded();

	/**
	 * Returns what kind of changes are produced by this reader.
	 *
	 * @see ChangelogRow.Kind
	 */
	ChangelogMode getChangelogMode();

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Creates the type information describing the internal format of produced {@link ChangelogRow}s.
		 *
		 * <p>Considers computed columns as part of the schema if {@link SupportsComputedColumnPushDown}
		 * is implemented and enabled.
		 */
		TypeInformation<ChangelogRow> createChangelogRowTypeInfo(TableSchema schema);

		/**
		 * Creates a producer that generates instances of {@link ChangelogRow} during runtime.
		 *
		 * <p>Generates computed columns if {@link SupportsComputedColumnPushDown} is implemented and
		 * enabled.
		 */
		ChangelogRowProducer createChangelogRowProducer(TableSchema schema);

		/**
		 * Creates the produced data type of the given schema.
		 *
		 * <p>Ignores non-persisted computed columns.
		 */
		DataType createProducedDataType(TableSchema schema);

		/**
		 * Creates a runtime data format converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 */
		DataFormatConverter createDataFormatConverter(DataType producedDataType);
	}

	interface ChangelogRowProducer extends FormatConverter {

		/**
		 * Produces a changelog row of a given kind with the columns of the given internal row format.
		 *
		 * <p>Generates computed columns in addition to columns of the internal ROW format if configured
		 * to do so.
		 */
		ChangelogRow toInternal(ChangelogRow.Kind kind, Object internalRowFormat);
	}

	interface DataFormatConverter extends FormatConverter {

		/**
		 * Converts the given object into an internal data format.
		 */
		@Nullable Object toInternal(@Nullable Object externalFormat);
	}
}
