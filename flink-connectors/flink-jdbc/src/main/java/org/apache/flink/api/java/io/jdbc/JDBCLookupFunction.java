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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connectors.jdbc.JdbcLookupFunction;
import org.apache.flink.connectors.jdbc.JdbcLookupOptions;
import org.apache.flink.connectors.jdbc.JdbcOptions;
import org.apache.flink.table.functions.TableFunction;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TableFunction} to query fields from JDBC by keys.
 * The query template like:
 * <PRE>
 * SELECT c, d, e, f from T where a = ? and b = ?
 * </PRE>
 *
 * <p>Support cache the result to avoid frequent accessing to remote databases.
 * 1.The cacheMaxSize is -1 means not use cache.
 * 2.For real-time data, you need to set the TTL of cache.
 *
 * @deprecated Please use {@link JdbcLookupFunction}, Flink proposes class name start with "Jdbc" rather than "JDBC".
 */
@Deprecated
public class JDBCLookupFunction extends JdbcLookupFunction {

	public JDBCLookupFunction(
			JdbcOptions options,
			JdbcLookupOptions lookupOptions,
			String[] fieldNames,
			TypeInformation[] fieldTypes,
			String[] keyNames) {
		super(options, lookupOptions, fieldNames, fieldTypes, keyNames);
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for a {@link JdbcLookupFunction}.
	 */
	public static class Builder extends JdbcLookupFunction.Builder {

		private JDBCOptions options;
		private JDBCLookupOptions lookupOptions;

		/**
		 * required, jdbc options.
		 */
		public Builder setOptions(JDBCOptions options) {
			this.options = options;
			return this;
		}

		/**
		 * optional, lookup related options.
		 */
		public Builder setLookupOptions(JDBCLookupOptions lookupOptions) {
			this.lookupOptions = lookupOptions;
			return this;
		}

		/**
		 * required, field names of this jdbc table.
		 */
		public Builder setFieldNames(String[] fieldNames) {
			this.fieldNames = fieldNames;
			return this;
		}

		/**
		 * required, field types of this jdbc table.
		 */
		public Builder setFieldTypes(TypeInformation[] fieldTypes) {
			this.fieldTypes = fieldTypes;
			return this;
		}

		/**
		 * required, key names to query this jdbc table.
		 */
		public Builder setKeyNames(String[] keyNames) {
			this.keyNames = keyNames;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JdbcLookupFunction
		 */
		public JDBCLookupFunction build() {
			checkNotNull(options, "No JdbcOptions supplied.");
			if (lookupOptions == null) {
				lookupOptions = JDBCLookupOptions.builder().build();
			}
			checkNotNull(fieldNames, "No fieldNames supplied.");
			checkNotNull(fieldTypes, "No fieldTypes supplied.");
			checkNotNull(keyNames, "No keyNames supplied.");

			return new JDBCLookupFunction(options, lookupOptions, fieldNames, fieldTypes, keyNames);
		}
	}

}
