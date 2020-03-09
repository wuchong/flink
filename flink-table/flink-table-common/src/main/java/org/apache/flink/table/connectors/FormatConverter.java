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

import org.apache.flink.table.connectors.sinks.SupportsChangelogWriting;
import org.apache.flink.table.connectors.sources.SupportsChangelogReading;

import java.io.Serializable;

/**
 * Converts between the internal formats of the Table & SQL API and external produced/consumed formats.
 */
public interface FormatConverter extends Serializable {

	/**
	 * Initializes the producer during runtime. This should be called in the {@code open()} method
	 * of a runtime class.
	 */
	void open(Context context);

	/**
	 * Context for format conversions in {@link SupportsChangelogReading} and {@link SupportsChangelogWriting}.
	 */
	interface Context {

		// empty for now until we have an understanding what is needed

		static Context empty() {
			return new EmptyFormatConverterContext();
		}
	}
}
