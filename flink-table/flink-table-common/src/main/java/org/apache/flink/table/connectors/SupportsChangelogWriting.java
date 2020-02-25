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

	// --------------------------------------------------------------------------------------------
	// Helper Interfaces
	// --------------------------------------------------------------------------------------------

	interface Context {

		/**
		 * Returns the user code class loader.
		 */
		ClassLoader getUserClassLoader();

		/**
		 * Creates a runtime data format converter that converts Flink's internal data structures to
		 * data of the given {@link DataType}.
		 */
		DataFormatConverter createDataFormatConverter(DataType consumedDataType);
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
