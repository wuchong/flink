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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A {@link TableReader} that scans rows of an external storage system.
 *
 * <p>It is not tied to any runtime implementation, use concrete sub-interfaces instead.
 */
public interface ScanTableReader extends TableReader {

	/**
	 * Whether the data is bounded or not.
	 */
	boolean isBounded();

	/**
	 * Returns what kind of changes are produced by this reader.
	 *
	 * @see ChangelogRow.Kind
	 */
	ChangeMode getChangeMode();

	// future work...
	// boolean isVectorized();

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Creates type information describing the internal format of the given {@link DataType}.
		 */
		TypeInformation<Object> createTypeInformation(DataType producedDataType);

		/**
		 * Creates type information describing the internal format of the given ROW {@link DataType}.
		 */
		TypeInformation<ChangelogRow> createRowTypeInformation(DataType producedRowDataType);

		/**
		 * Creates a runtime data format converter that converts data of the given {@link DataType}
		 * to Flink's internal data structures.
		 */
		DataFormatConverter createDataFormatConverter(DataType producedDataType);

		/**
		 * Creates a runtime row producer. Useful in cases where additional logic is required within
		 * field data format conversion.
		 *
		 * <p>Note: This is low-level API. For most of the cases, {@link #createDataFormatConverter(DataType)}
		 * should be sufficient.
		 */
		RowFormatProducer createRowFormatProducer(DataType producedDataType);

		/**
		 * Creates a runtime array producer. Useful in cases where additional logic is required within
		 * element data format conversion.
		 *
		 * <p>Note: This is low-level API. For most of the cases, {@link #createDataFormatConverter(DataType)}
		 * should be sufficient.
		 */
		ArrayFormatProducer createArrayFormatProducer(DataType producedDataType);

		/**
		 * Creates a runtime map producer. Useful in cases where additional logic is required within
		 * entry data format conversion.
		 *
		 * <p>Note: This is low-level API. For most of the cases, {@link #createDataFormatConverter(DataType)}
		 * should be sufficient.
		 */
		MapFormatProducer createMapFormatProducer(ArrayFormatProducer keyArrayProducer, ArrayFormatProducer valueArrayProducer);

		// future work...
		// VectorizedRowProducer createVectorizedRowProducer(...);
	}

	interface DataFormatConverter extends FormatConverter, Serializable {

		/**
		 * Converts the given object into an internal data format. If this is the top-level row, it
		 * can be safely casted in a {@link ChangelogRow}.
		 */
		@Nullable Object toInternal(@Nullable Object externalFormat);

		/**
		 * Converts the given object into an internal row data format with a corresponding kind of
		 * change. It assumes that the configured data type of this converter is a row type.
		 */
		@Nullable ChangelogRow toInternalRow(ChangelogRow.Kind kind, @Nullable Object externalFormat);
	}

	interface RowFormatProducer extends FormatConverter, Serializable {

		void setKind(ChangelogRow.Kind kind);

		void setField(int fieldPos, @Nullable Object internalFormat);

		void setField(int fieldPos, boolean value);

		void setField(int fieldPos, byte value);

		void setField(int fieldPos, short value);

		void setField(int fieldPos, int value);

		void setField(int fieldPos, long value);

		void setField(int fieldPos, float value);

		void setField(int fieldPos, double value);

		/**
		 * Finalizes and builds the row using an internal data format.
		 */
		ChangelogRow toInternal();
	}

	interface ArrayFormatProducer extends FormatConverter, Serializable {

		/**
		 * Allocates a new array with the given length.
		 *
		 * <p>Make sure to call this method before calling any setters or {@link #toInternal()}.
		 */
		void allocate(int length);

		void setElement(int elementPos, @Nullable Object internalFormat);

		void setElement(int elementPos, boolean value);

		void setElement(int elementPos, byte value);

		void setElement(int elementPos, short value);

		void setElement(int elementPos, int value);

		void setElement(int elementPos, long value);

		void setElement(int elementPos, float value);

		void setElement(int elementPos, double value);

		/**
		 * Finalizes and builds the array using an internal data format.
		 */
		Object toInternal();
	}

	interface MapFormatProducer extends FormatConverter, Serializable {

		/**
		 * {@inheritDoc}
		 *
		 * <p>Forwards the call to underlying array producers.
		 */
		void init(Context context);

		/**
		 * Finalizes and builds the map using an internal data format.
		 *
		 * <p>Forwards the call to underlying array producers.
		 */
		Object toInternal();
	}
}
