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
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

/**
 * A {@link WritingAbility} that writes a changelog of rows to an external storage system.
 */
@PublicEvolving
public interface SupportsChangelogWriting extends WritingAbility {

	/**
	 * Returns the {@link ChangelogMode} that this writer consumes.
	 *
	 * <p>The runtime can make suggestions but the writer has the final decision what it requires. If
	 * the runtime does not support this mode, it will throw an error.
	 */
	ChangelogMode getChangelogMode(ChangelogMode requestedMode);

	/**
	 * Returns the actual implementation for writing the data.
	 */
	ChangelogWriter getChangelogWriter(Context context);

	/**
	 * Returns whether computed columns can be pushed into the {@link SupportsChangelogWriting.ChangelogWriter}
	 * or if they need to be removed in a preceding projection before the sink.
	 *
	 * <p>Disabling the computed column push down is only valid for implementations that don't use the
	 * recommended {@link SupportsChangelogWriting.ChangelogRowConsumer}.
	 */
	default boolean supportsComputedColumnPushDown() {
		return true;
	}

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Returns the user code class loader.
		 */
		ClassLoader getUserClassLoader();

		/**
		 * Creates a consumer that accesses instances of {@link ChangelogRow} during runtime.
		 *
		 * <p>Removes computed columns if necessary.
		 */
		ChangelogRowConsumer createChangelogRowConsumer();

		/**
		 * Creates a runtime data format converter that converts Flink's internal data structures to
		 * data of the given {@link DataType}.
		 */
		DataFormatConverter createDataFormatConverter(DataType consumedDataType);
	}

	interface ChangelogRowConsumer extends FormatConverter {

		/**
		 * Unwraps the columns of a row in internal format from a changelog row.
		 *
		 * <p>Removes computed columns if necessary.
		 */
		Object unwrapInternalRow(ChangelogRow row);
	}

	interface DataFormatConverter extends FormatConverter {

		/**
		 * Converts the given object into an external data format.
		 */
		@Nullable Object toExternal(@Nullable Object internalFormat);
	}

	interface ChangelogWriter {
		// marker interface
	}
}
